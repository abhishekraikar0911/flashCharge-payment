from enum import Enum
from io import BytesIO
import json
from typing import List, Tuple
from aio_pika import connect
from aio_pika.abc import AbstractExchange, AbstractIncomingMessage
from fastapi import FastAPI
from pydantic import BaseModel
from pydantic_core import ValidationError
import requests
from sqlalchemy.orm import Session
import stripe
import qrcode

from config import Config
from logging import debug, exception, info, warning
from db.init_db import (
    MessageInfo as MessageInfoModel,
    get_db,
    Checkout as CheckoutModel,
    Evse as EvseModel,
    Location as LocationModel,
    Tariff as TariffModel,
)

from integrations.integration import FileIntegration, OcppIntegration
from schemas.status_notification import StatusNotificationRequest
from schemas.transaction_event import (
    MeasurandEnumType,
    TransactionEventEnumType,
    TriggerReasonEnumType,
    TransactionEventRequest,
    Ocpp16StartTransactionRequest,
    Ocpp16StartTransactionResponse,
    Ocpp16StopTransactionRequest,
)


class CitrineOsEventAction(str, Enum):
    TRANSACTIONEVENT = "TransactionEvent"
    STATUSNOTIFICATION = "StatusNotification"
    STARTTRANSACTION = "StartTransaction"
    STOPTRANSACTION = "StopTransaction"


class CitrineOSevent(BaseModel):
    action: CitrineOsEventAction
    payload: dict


class CitrineOSeventState:
    REQUEST = "1"
    RESPONSE = "2"


class CitrineOSeventHeaders(BaseModel):
    stationId: str


class CitrineOSIntegration(OcppIntegration):
    def __init__(self, fileIntegration: FileIntegration):
        self.fileIntegration = fileIntegration

    async def create_authorization(
        self,
        idToken: str,
        idTokenType: str,
        additionalInfo: List[Tuple[str, str]],
        app: FastAPI = None,
    ):
        idToken_obj = {
            "idToken": idToken,
            "type": idTokenType,
            "additionalInfo": [
                {"additionalIdToken": item[0], "type": item[1]}
                for item in additionalInfo
            ],
        }
        request_body = {
            "idToken": idToken_obj,
            "idTokenInfo": {
                "status": "Accepted",
            },
        }
        module = "evdriver"
        data = "authorization"
        url_path = f"{module}/{data}"
        request_url = (
            f"{Config.CITRINEOS_DATA_API_URL}/{url_path}"
            f"?idToken={idToken_obj['idToken']}"
            f"&type={idToken_obj['type']}"
        )

        response = requests.put(request_url, json=request_body)
        if response.status_code == 200:
            return request_body

        # Fallback: REST endpoint not available in this CitrineOS version.
        # Insert authorization directly into the CitrineOS Core database.
        info(
            " [CitrineOS] REST authorization endpoint returned %r, falling back to direct DB insert.",
            response.status_code,
        )
        import json as _json
        from sqlalchemy import create_engine, text
        from config import Config as _Config

        try:
            core_engine = create_engine(
                f"postgresql://{_Config.DB_USER}:{_Config.DB_PASSWORD}"
                f"@{_Config.DB_HOST}:{_Config.DB_PORT}/{_Config.DB_DATABASE}"
            )
            additional_info_json = _json.dumps(
                [{"additionalIdToken": item[0], "type": item[1]} for item in additionalInfo]
            )
            with core_engine.connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO "Authorizations"
                            ("idToken", "idTokenType", "additionalInfo", "status", "tenantId",
                             "createdAt", "updatedAt")
                        VALUES
                            (:idToken, :idTokenType, CAST(:additionalInfo AS jsonb), 'Accepted', 1,
                             NOW(), NOW())
                        ON CONFLICT ("idToken", "idTokenType")
                        DO UPDATE SET "status" = 'Accepted', "updatedAt" = NOW(),
                            "additionalInfo" = CAST(:additionalInfo AS jsonb)
                        """
                    ),
                    {
                        "idToken": idToken_obj["idToken"],
                        "idTokenType": idTokenType,
                        "additionalInfo": additional_info_json,
                    },
                )
                conn.commit()
            info(" [CitrineOS] Authorization inserted directly into DB for idToken: %r", idToken)
            return request_body
        except Exception as e:
            exception(" [CitrineOS] DB fallback for authorization also failed: %r", e)
            return

    def send_citrineos_message(
        self, station_id: str, tenant_id: str, url_path: str, json_payload: str
    ) -> requests.Response:
        base_url = Config.CITRINEOS_MESSAGE_API_URL.rstrip("/")
        request_url = (
            f"{base_url}/{url_path}"
            f"?identifier={station_id}"
            f"&tenantId={tenant_id}"
        )
        info(" [CitrineOS] Sending message to: %r", request_url)
        response = requests.post(request_url, json=json_payload)
        info(" [CitrineOS] Response: %r %r", response.status_code, response.text[:200])
        return response


    async def receive_events(self, app: FastAPI = None) -> None:
        # Perform connection
        connection = await connect(
            ssl=Config.MESSAGE_BROKER_SSL_ACTIVE,
            host=Config.MESSAGE_BROKER_HOST,
            port=Config.MESSAGE_BROKER_PORT,
            login=Config.MESSAGE_BROKER_USER,
            password=Config.MESSAGE_BROKER_PASSWORD,
            virtualhost=Config.MESSAGE_BROKER_VHOST,
        )

        # Creating a channel
        channel = await connection.channel()
        exchange: AbstractExchange = await channel.declare_exchange(
            name=Config.MESSAGE_BROKER_EXCHANGE_NAME,
            type=Config.MESSAGE_BROKER_EXCHANGE_TYPE,
        )

        # Declaring queue
        queue = await channel.declare_queue(
            Config.MESSAGE_BROKER_EVENT_CONSUMER_QUEUE_NAME, durable=True
        )

        # Bind headers
        arguments_list = [
            {
                "action": "TransactionEvent",
                "state": "1",
                "x-match": "all",
            },
            {
                "action": "StatusNotification",
                "state": "1",
                "x-match": "all",
            },
            {
                "action": "StartTransaction",
                "state": "1",
                "x-match": "all",
            },
            {
                # Subscribe to StartTransaction RESPONSES (state=2) to capture transactionId
                "action": "StartTransaction",
                "state": "2",
                "x-match": "all",
            },
            {
                "action": "StopTransaction",
                "state": "1",
                "x-match": "all",
            },
        ]
        for arguments in arguments_list:
            await queue.bind(
                exchange=exchange,
                routing_key="",
                arguments=arguments,
            )

        info(" [CitrineOS] Awaiting events with keys: %r ", arguments_list.__str__())

        # Start listening the queue with name 'hello'
        async with queue.iterator() as qiterator:
            message: AbstractIncomingMessage
            async for message in qiterator:
                try:
                    async with (
                        message.process()
                    ):  # Processor acknowledges messages implicitly
                        debug(
                            f" [CitrineOS] event_message({message.headers.__str__()})"
                        )
                        await self.process_incoming_event(
                            event_message=message, exchange=exchange
                        )
                        debug(
                            " [CitrineOS] Event processed successfully: %r",
                            message.headers.__str__(),
                        )
                except Exception:
                    exception(" [CitrineOS] Processing error for message %r", message)

    async def process_incoming_event(
        self, event_message: AbstractIncomingMessage, exchange: AbstractExchange
    ) -> None:
        try:
            decoded_body = event_message.body.decode()
            citrine_os_event = CitrineOSevent(**json.loads(decoded_body))
            db: Session = next(get_db())

            if citrine_os_event.action == CitrineOsEventAction.TRANSACTIONEVENT:
                citrine_os_event_headers = CitrineOSeventHeaders(
                    **event_message.headers
                )
                transaction_event = TransactionEventRequest(**citrine_os_event.payload)
                if (
                    transaction_event.eventType == TransactionEventEnumType.Started
                    or transaction_event.triggerReason
                    == TriggerReasonEnumType.RemoteStart
                ):
                    await self.process_transaction_started(
                        db=db,
                        transaction_event=transaction_event,
                        citrine_os_event_headers=citrine_os_event_headers,
                    )
                elif transaction_event.eventType == TransactionEventEnumType.Updated:
                    await self.process_transaction_updated(
                        db=db,
                        transaction_event=transaction_event,
                    )
                elif transaction_event.eventType == TransactionEventEnumType.Ended:
                    await self.process_transaction_ended(
                        db=db,
                        transaction_event=transaction_event,
                    )
                return
            elif citrine_os_event.action == CitrineOsEventAction.STARTTRANSACTION:
                citrine_os_event_headers = CitrineOSeventHeaders(
                    **event_message.headers
                )
                state = event_message.headers.get("state", CitrineOSeventState.REQUEST)
                if state == CitrineOSeventState.RESPONSE:
                    # StartTransaction RESPONSE from charger — capture transactionId
                    start_transaction_response = Ocpp16StartTransactionResponse(**citrine_os_event.payload)
                    await self.process_ocpp16_start_transaction_response(
                        db=db,
                        start_transaction_response=start_transaction_response,
                        citrine_os_event_headers=citrine_os_event_headers,
                        correlation_id=event_message.headers.get("correlationId"),
                    )
                else:
                    # StartTransaction REQUEST from charger to CSMS — store correlation_id
                    start_transaction = Ocpp16StartTransactionRequest(**citrine_os_event.payload)
                    await self.process_ocpp16_start_transaction(
                        db=db,
                        start_transaction=start_transaction,
                        citrine_os_event_headers=citrine_os_event_headers,
                        correlation_id=event_message.headers.get("correlationId"),
                    )
                return
            elif citrine_os_event.action == CitrineOsEventAction.STOPTRANSACTION:
                citrine_os_event_headers = CitrineOSeventHeaders(
                    **event_message.headers
                )
                stop_transaction = Ocpp16StopTransactionRequest(**citrine_os_event.payload)
                await self.process_ocpp16_stop_transaction(
                    db=db,
                    stop_transaction=stop_transaction,
                    citrine_os_event_headers=citrine_os_event_headers,
                )
                return
            elif citrine_os_event.action == CitrineOsEventAction.STATUSNOTIFICATION:
                citrine_os_event_headers = CitrineOSeventHeaders(
                    **event_message.headers
                )
                status_notification = StatusNotificationRequest(
                    **citrine_os_event.payload
                )
                await self.process_status_notification(
                    db=db,
                    status_notification=status_notification,
                    citrine_os_event_headers=citrine_os_event_headers,
                )
        except ValidationError as e:
            if e.title == CitrineOSevent.__name__:
                debug(
                    " [CitrineOS] Received event which is not valid CitrineOS event: %r",
                    e.errors(),
                )
            elif e.title == TransactionEventRequest.__name__:
                warning(
                    " [CitrineOS] Received valid TransactionEvent, but fields missing: %r",
                    e.errors(),
                )
            else:
                raise e
        except Exception as e:
            exception(" [CitrineOS] Processing error for incoming event: %r", e.__str__)
            raise e

    async def _get_checkout_by_event(
        self,
        db: Session,
        id_tag: str | None = None,
        remote_start_id: int | None = None,
        transaction_id: int | None = None,
        station_id: str | None = None,
        protocol: str = "unknown",
    ) -> CheckoutModel | None:
        checkout_id = None

        if remote_start_id:
            checkout_id = remote_start_id
        elif id_tag and id_tag.startswith(Config.OCPP_REMOTESTART_IDTAG_PREFIX):
            try:
                # Strictly parse checkout_id from prefix
                checkout_id_str = id_tag[len(Config.OCPP_REMOTESTART_IDTAG_PREFIX) :]
                checkout_id = int(checkout_id_str)
            except (ValueError, IndexError):
                warning(f" [CitrineOS] Could not extract checkout ID from idTag: {id_tag}")

        # Fallback for OCPP 1.6 StopTransaction which might not have idTag
        if checkout_id is None and transaction_id is not None and protocol == "ocpp1.6":
            info(f" [CitrineOS] Attempting fallback correlation for transactionId={transaction_id}")
            from sqlalchemy import create_engine, text
            try:
                core_engine = create_engine(
                    f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_DATABASE}"
                )
                with core_engine.connect() as conn:
                    result = conn.execute(
                        text(
                            """
                            SELECT a."idToken" 
                            FROM "Transactions" t
                            JOIN "Authorizations" a ON t."authorizationId" = a.id
                            WHERE t."transactionId" = :tx_id AND t."stationId" = :station_id
                            """
                        ),
                        {"tx_id": str(transaction_id), "station_id": station_id},
                    ).fetchone()
                    if result and result[0].startswith(Config.OCPP_REMOTESTART_IDTAG_PREFIX):
                        id_tag = result[0]
                        checkout_id_str = id_tag[len(Config.OCPP_REMOTESTART_IDTAG_PREFIX) :]
                        checkout_id = int(checkout_id_str)
                        info(f" [CitrineOS] Fallback success: transactionId={transaction_id} -> idTag={id_tag} -> checkout_id={checkout_id}")
            except Exception as e:
                warning(f" [CitrineOS] Fallback correlation failed: {e}")

        if checkout_id:
            db_checkout = db.query(CheckoutModel).filter(CheckoutModel.id == checkout_id).first()
            if db_checkout:
                info(
                    f" [Billing Correlation] Protocol={protocol} idTag={id_tag} -> checkout_id={checkout_id}"
                )
                return db_checkout

        warning(f" [CitrineOS] Checkout not found for event (idTag={id_tag}, remoteStartId={remote_start_id})")
        return None

    async def process_transaction_started(
        self,
        db: Session,
        transaction_event: TransactionEventRequest,
        citrine_os_event_headers: CitrineOSeventHeaders,
    ) -> None:
        triggerReasonNoAuthArray = [
            TriggerReasonEnumType.CablePluggedIn,
            TriggerReasonEnumType.SignedDataReceived,
            TriggerReasonEnumType.EVDetected,
        ]
        if (
            Config.CITRINEOS_SCAN_AND_CHARGE
            and transaction_event.triggerReason in triggerReasonNoAuthArray
            and transaction_event.idToken is None
        ):
            await self.process_transaction_started_scan_and_charge(
                db=db,
                transaction_event=transaction_event,
                citrine_os_event_headers=citrine_os_event_headers,
            )
        else:
            await self.process_transaction_started_remote(
                db=db,
                transaction_event=transaction_event
            )

    async def process_transaction_started_scan_and_charge(
        self,
        db: Session,
        transaction_event: TransactionEventRequest,
        citrine_os_event_headers: CitrineOSeventHeaders,
    ) -> None:
        transactionId = transaction_event.transactionInfo.transactionId
        stationId = citrine_os_event_headers.stationId

        # If pricing is found to vary by evse, we need to change triggerReasonNoAuthArray to mandate events that know the evse
        # Then add a filter below, EvseModel.ocpp_evse_id == transaction_event.evse.id
        evse = db.query(EvseModel).filter(EvseModel.station_id == stationId).first()
        if evse is None:
            raise Exception("EVSE not found")

        tariff = (
            db.query(TariffModel)
            .filter(TariffModel.id == evse.connectors[0].tariff_id)
            .first()
        )
        if tariff is None:
            raise Exception("No Tariff for EVSE found")

        location = (
            db.query(LocationModel).filter(LocationModel.id == evse.location_id).first()
        )
        if location is None:
            raise Exception("No Location for EVSE found")

        db_checkout = CheckoutModel(
            connector_id=evse.connectors[0].id, tariff_id=tariff.id
        )
        db_checkout = self.update_checkout_with_meter_values(
            transaction_event=transaction_event, db_checkout=db_checkout
        )
        db.add(db_checkout)
        db.commit()
        db.refresh(db_checkout)

        if tariff.stripe_price_id is None:
            price = stripe.Price.create(
                currency=tariff.currency.lower(),
                metadata={"tariffId": tariff.id},
                product_data={"name": "Charging Session Authorization Amount"},
                tax_behavior="inclusive",
                unit_amount=int(tariff.authorization_amount * 100),
            )
            tariff.stripe_price_id = price.id
            db.add(tariff)
            db.commit()

        payment_link_url = await self.create_payment_link(
            stripe_price_id=tariff.stripe_price_id,
            stripe_account_id=location.operator.stripe_account_id,
            stationId=stationId,
            evseId=evse.evse_id,
            transactionId=transactionId,
            checkoutId=db_checkout.id,
        )

        qr_code_img = qrcode.make(payment_link_url)
        # Save the image to an in-memory buffer
        buffer = BytesIO()
        debug(type(qr_code_img))
        debug(dir(qr_code_img))
        qr_code_img.save(buffer)
        buffer.seek(0)  # Rewind the buffer to the beginning

        qr_code_img_url = self.fileIntegration.upload_file(
            buffer,
            "image/png",
            f"qrcode_{stationId}_{transactionId}.png",
            f"QRCode_{stationId}_{transactionId}",
        )

        mostRecentMessageInfoForStation = (
            db.query(MessageInfoModel)
            .filter(MessageInfoModel.stationId == stationId)
            .order_by(MessageInfoModel.id.desc())
            .first()
        )
        nextMessageId = (
            0
            if mostRecentMessageInfoForStation is None
            else mostRecentMessageInfoForStation.id + 1
        )
        set_display_message_request = {
            "message": {
                "id": nextMessageId,
                "priority": "AlwaysFront",
                "transactionId": transactionId,
                "message": {"format": "URI", "content": qr_code_img_url},
            }
        }
        citrineos_module = (
            "configuration"  # TODO set up programatic way to resolve module from action
        )
        action = "setDisplayMessage"

        self.send_citrineos_message(
            station_id=stationId,
            tenant_id=evse.tenant_id,
            url_path=f"{citrineos_module}/{action}",
            json_payload=set_display_message_request,
        )
        db_checkout.qr_code_message_id = nextMessageId
        db.add(db_checkout)
        db.commit()

    async def create_payment_link(
        self,
        stripe_price_id: str,
        stripe_account_id: str,
        stationId: str,
        evseId: str,
        transactionId: str,
        checkoutId: int,
    ) -> str:
        transactionPaymentLink = stripe.PaymentLink.create(
            after_completion={
                "redirect": {
                    "url": f"{Config.CLIENT_URL}/charging/{evseId}/{checkoutId}"
                },
                "type": "redirect",
            },
            line_items=[
                {
                    "price": stripe_price_id,
                    "quantity": 1,
                }
            ],
            metadata={
                "stationId": stationId,
                "transactionId": transactionId,
                "checkoutId": checkoutId,
            },
            payment_intent_data={
                "capture_method": "manual",
            },
            payment_method_types=["card"],
            restrictions={"completed_sessions": {"limit": int(1)}},
            stripe_account=stripe_account_id,
        )
        return transactionPaymentLink.url

    async def process_transaction_started_remote(
        self, db: Session, transaction_event: TransactionEventRequest
    ) -> None:
        db_checkout = await self._get_checkout_by_event(
            db=db,
            id_tag=transaction_event.idToken.idToken if transaction_event.idToken else None,
            remote_start_id=transaction_event.transactionInfo.remoteStartId,
            protocol="ocpp2.0.1",
        )
        if db_checkout is None:
            return

        db_checkout.ocpp_protocol = "ocpp2.0.1"
        db_checkout.transaction_start_time = transaction_event.timestamp

        db_checkout = self.update_checkout_with_meter_values(
            transaction_event=transaction_event, db_checkout=db_checkout
        )
        db.add(db_checkout)
        db.commit()
        db.refresh(db_checkout)
        return

    async def process_transaction_updated(
        self, db: Session, transaction_event: TransactionEventRequest
    ) -> None:
        db_checkout = await self._get_checkout_by_event(
            db=db,
            id_tag=transaction_event.idToken.idToken if transaction_event.idToken else None,
            remote_start_id=transaction_event.transactionInfo.remoteStartId,
            protocol="ocpp2.0.1",
        )
        if db_checkout is None:
            return

        db_checkout.ocpp_protocol = "ocpp2.0.1"
        db_checkout = self.update_checkout_with_meter_values(
            transaction_event=transaction_event, db_checkout=db_checkout
        )
        db.add(db_checkout)
        db.commit()
        db.refresh(db_checkout)
        return

    async def process_transaction_ended(
        self, db: Session, transaction_event: TransactionEventRequest
    ) -> None:
        db_checkout = await self._get_checkout_by_event(
            db=db,
            id_tag=transaction_event.idToken.idToken if transaction_event.idToken else None,
            remote_start_id=transaction_event.transactionInfo.remoteStartId,
            protocol="ocpp2.0.1",
        )
        if db_checkout is None:
            return

        db_checkout.ocpp_protocol = "ocpp2.0.1"
        db_checkout = self.update_checkout_with_meter_values(
            transaction_event=transaction_event, db_checkout=db_checkout
        )
        db_checkout.transaction_end_time = transaction_event.timestamp
        db.add(db_checkout)
        db.commit()
        db.refresh(db_checkout)

        await self.capture_payment_transaction(app=None, checkout_id=db_checkout.id)

        return

    async def process_ocpp16_start_transaction(
        self,
        db: Session,
        start_transaction: Ocpp16StartTransactionRequest,
        citrine_os_event_headers: CitrineOSeventHeaders,
        correlation_id: str | None = None,
    ) -> None:
        db_checkout = await self._get_checkout_by_event(
            db=db, id_tag=start_transaction.idTag, protocol="ocpp1.6"
        )
        if db_checkout is None:
            return

        db_checkout.ocpp_protocol = "ocpp1.6"
        db_checkout.transaction_start_time = start_transaction.timestamp
        # Initial meter reading for 1.6
        db_checkout.transaction_last_meter_reading = start_transaction.meterStart / 1000  # Wh to kWh
        db_checkout.transaction_kwh = 0
        # Store correlation_id so we can link the response (with transactionId) back to this checkout
        if correlation_id:
            db_checkout.correlation_id = correlation_id
            info(f" [CitrineOS] OCPP1.6 StartTransaction request received, correlation_id={correlation_id}")
        db.add(db_checkout)
        db.commit()
        db.refresh(db_checkout)
        return

    async def process_ocpp16_start_transaction_response(
        self,
        db: Session,
        start_transaction_response: Ocpp16StartTransactionResponse,
        citrine_os_event_headers: CitrineOSeventHeaders,
        correlation_id: str | None = None,
    ) -> None:
        """Handles the StartTransaction RESPONSE from the charger.
        The response contains the transactionId assigned by the charger,
        which is required to stop the transaction later."""
        if not correlation_id:
            warning(" [CitrineOS] StartTransaction response received without correlation_id — cannot link to checkout.")
            return

        # Find the Checkout via the correlation_id stored during the request
        db_checkout = (
            db.query(CheckoutModel)
            .filter(CheckoutModel.correlation_id == correlation_id)
            .first()
        )
        if db_checkout is None:
            warning(f" [CitrineOS] No checkout found for correlation_id={correlation_id} in StartTransaction response")
            return

        # Store the transactionId from the charger's response
        transaction_id = str(start_transaction_response.transactionId)
        db_checkout.transaction_id = transaction_id
        db.add(db_checkout)
        db.commit()
        db.refresh(db_checkout)
        info(f" [CitrineOS] Transaction ID captured: checkout_id={db_checkout.id} transactionId={transaction_id} correlation_id={correlation_id}")
        return

    async def process_ocpp16_stop_transaction(
        self,
        db: Session,
        stop_transaction: Ocpp16StopTransactionRequest,
        citrine_os_event_headers: CitrineOSeventHeaders,
    ) -> None:
        db_checkout = await self._get_checkout_by_event(
            db=db,
            id_tag=stop_transaction.idTag,
            transaction_id=stop_transaction.transactionId,
            station_id=citrine_os_event_headers.stationId,
            protocol="ocpp1.6",
        )
        if db_checkout is None:
            return

        db_checkout.ocpp_protocol = "ocpp1.6"
        db_checkout.transaction_end_time = stop_transaction.timestamp
        
        # Calculate final kWh for 1.6
        if stop_transaction.meterStop is not None:
            new_kwh_value = stop_transaction.meterStop / 1000
            if db_checkout.transaction_last_meter_reading is not None:
                db_checkout.transaction_kwh = new_kwh_value - db_checkout.transaction_last_meter_reading
            db_checkout.transaction_last_meter_reading = new_kwh_value

        db.add(db_checkout)
        db.commit()
        db.refresh(db_checkout)

        await self.capture_payment_transaction(app=None, checkout_id=db_checkout.id)
        return

    def update_checkout_with_meter_values(
        self, transaction_event: TransactionEventRequest, db_checkout: CheckoutModel
    ) -> CheckoutModel:
        if (
            transaction_event.meterValue is not None
            and len(transaction_event.meterValue) > 0
        ):
            latest_meter_value = transaction_event.meterValue[
                len(transaction_event.meterValue) - 1
            ]
            for sampled_value in latest_meter_value.sampledValue:
                if (
                    sampled_value.measurand
                    == MeasurandEnumType.EnergyActiveImportRegister
                    and sampled_value.phase is None
                ):
                    new_kwh_value = sampled_value.value
                    if (
                        sampled_value.unitOfMeasure.unit is None
                        or sampled_value.unitOfMeasure.unit == "Wh"
                    ):
                        new_kwh_value = new_kwh_value / 1000
                    if sampled_value.unitOfMeasure.multiplier is not None:
                        new_kwh_value = (
                            new_kwh_value * 10**sampled_value.unitOfMeasure.multiplier
                        )
                    if db_checkout.transaction_last_meter_reading is None:
                        db_checkout.transaction_kwh = 0
                    if db_checkout.transaction_last_meter_reading is not None:
                        db_checkout.transaction_kwh += (
                            new_kwh_value - db_checkout.transaction_last_meter_reading
                        )
                    db_checkout.transaction_last_meter_reading = new_kwh_value

                elif (
                    sampled_value.measurand == MeasurandEnumType.PowerActiveImport
                    and sampled_value.phase is None
                ):
                    new_power_value = sampled_value.value
                    if (
                        sampled_value.unitOfMeasure.unit is None
                        or sampled_value.unitOfMeasure.unit == "W"
                    ):
                        new_power_value = new_power_value / 1000
                    if sampled_value.unitOfMeasure.multiplier is not None:
                        new_power_value = (
                            new_power_value * 10**sampled_value.unitOfMeasure.multiplier
                        )
                    db_checkout.power_active_import = new_power_value

                elif (
                    sampled_value.measurand == MeasurandEnumType.SoC
                    and sampled_value.phase is None
                ):
                    new_soc_value = sampled_value.value
                    if sampled_value.unitOfMeasure.multiplier is not None:
                        new_soc_value = (
                            new_soc_value * 10**sampled_value.unitOfMeasure.multiplier
                        )
                    db_checkout.transaction_soc = new_soc_value
        return db_checkout

    async def process_status_notification(
        self,
        db: Session,
        status_notification: StatusNotificationRequest,
        citrine_os_event_headers: CitrineOSeventHeaders,
    ) -> None:
        db: Session = next(get_db())
        db_evse = (
            db.query(EvseModel)
            .filter(EvseModel.station_id == citrine_os_event_headers.stationId)
            .filter(EvseModel.ocpp_evse_id == status_notification.evseId)
            .first()
        )
        if db_evse is None:
            info(
                " [CitrineOS] EVSE not found for status notification event: %r",
                status_notification,
            )
            return

        db_evse.status = status_notification.connectorStatus
        db.add(db_evse)
        db.commit()
        db.refresh(db_evse)
        return
