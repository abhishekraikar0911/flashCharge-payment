import json
import time
from datetime import datetime, timezone, timedelta
from uuid import uuid4
import stripe
from fastapi import APIRouter, Depends, HTTPException, Header, Request
from logging import debug, exception, info, warning
from json import loads
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text

from config import Config
from db.init_db import Connector, Evse, Transaction, get_db, Checkout as CheckoutModel
from integrations.integration import OcppIntegration
from schemas.checkouts import RequestStartStopStatusEnumType

router = APIRouter()


@router.post("/stripe")
async def stripe_webhook(
    request: Request,
    STRIPE_SIGNATURE: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    event = None
    body = b""
    async for chunk in request.stream():
        body += chunk

    # charge.succeed is in connect_event_types, as we are using Stripe Standard accounts
    # for which the events are coming via the Connect-Webhook.
    # If we would use Stripe Express accounts, the events would be coming via the Account-Webhook
    account_event_types = []
    connect_event_types = ["checkout.session.completed"]
    try:
        event_type = loads(body.decode()).get("type")
        debug(" [*WEBHOOK*] Event type {}".format(event_type))
        if event_type in account_event_types:
            event = stripe.Webhook.construct_event(
                body, STRIPE_SIGNATURE, Config.STRIPE_ENDPOINT_SECRET_ACCOUNT
            )
        elif event_type in connect_event_types:
            event = stripe.Webhook.construct_event(
                body, STRIPE_SIGNATURE, Config.STRIPE_ENDPOINT_SECRET_CONNECT
            )
        else:
            debug(" [*WEBHOOK*] Unhandled event type {}".format(event_type))
            return {}
    except ValueError as e:
        raise e
    except stripe.error.SignatureVerificationError as e:
        raise e

    # Handle the event
    """
        Removed handling of account changes for now
    """
    if event.get("type") == "checkout.session.completed":
        # A Stripe Checkout session completed
        # Payment was successful, try to start a charging session
        checkout_session = event.get("data").get("object")
        paymentIntentId = checkout_session.get("payment_intent")

        metadata = checkout_session.get("metadata")
        checkoutId = metadata.get("checkoutId")
        stationId = metadata.get("stationId")
        transactionId = metadata.get("transactionId")
        debug(
            " [Stripe] stationId: %r, transactionId: %r, checkoutId: %r, paymentIntentId: %r",
            stationId,
            transactionId,
            checkoutId,
            paymentIntentId,
        )

        ocpp_integration: OcppIntegration = request.app.ocpp_integration
        db_checkout = (
            db.query(CheckoutModel).filter(CheckoutModel.id == checkoutId).first()
        )
        if db_checkout is None:
            raise HTTPException(
                status_code=404, detail="No checkout found for payment intent"
            )

        # --- FIX 1: Idempotency guard ---
        # Stripe may send the same webhook event multiple times (retries).
        # If this checkout was already successfully started, skip it silently.
        if db_checkout.remote_request_status == RequestStartStopStatusEnumType.ACCEPTED:
            info(
                " [Webhook] Duplicate event ignored — checkout %r already Accepted.",
                checkoutId,
            )
            return {}

        db_checkout.authorization_amount = checkout_session.get("amount_total")

        if paymentIntentId and checkoutId and not transactionId:
            await handle_web_portal(db, ocpp_integration, db_checkout, paymentIntentId)
        elif paymentIntentId and checkoutId and stationId and transactionId:
            await handle_scan_and_charge(
                db,
                ocpp_integration,
                db_checkout,
                paymentIntentId,
                stationId,
                transactionId,
            )
        else:
            raise HTTPException(status_code=404, detail="Metadata missing")

    return


async def handle_web_portal(
    db: Session,
    ocpp_integration: OcppIntegration,
    db_checkout: CheckoutModel,
    paymentIntentId: str,
):
    db_checkout.payment_intent_id = paymentIntentId
    db.add(db_checkout)
    db.commit()

    # TODO: Remove this part when CitrineOS is correctly saving the idToken from RemoteStartRequests.
    authorization = await ocpp_integration.create_authorization(
        f"{Config.OCPP_REMOTESTART_IDTAG_PREFIX}{db_checkout.id}",
        "Central",
        [
            (paymentIntentId, "PaymentIntentId"),
        ],
    )
    if authorization is None:
        debug(" [Stripe] Unable to create authorization for transaction")
        cancel_payment_intent(paymentIntentId)
        raise HTTPException(
            status_code=404, detail="Unable to create authorization for transaction"
        )

    idToken = authorization["idToken"]
    # OCPP 2.0.1 format (idToken object) + OCPP 1.6 format (idTag string)
    request_body = {
        "remoteStartId": db_checkout.id,
        "idToken": idToken,
        # OCPP 1.6 field: flat string idTag
        "idTag": idToken["idToken"],
    }

    db_connector = (
        db.query(Connector).filter(Connector.id == db_checkout.connector_id).first()
    )
    if db_connector is None:
        debug(
            " [CitrineOS] Connector not found for remote start request: %r",
            db_checkout.id,
        )
        return RequestStartStopStatusEnumType.REJECTED

    db_evse = db.query(Evse).filter(Evse.id == db_connector.evse_id).first()
    if db_evse is None:
        debug(
            " [CitrineOS] EVSE not found for remote start request: %r", db_checkout.id
        )
        return RequestStartStopStatusEnumType.REJECTED

    # OCPP 1.6 uses connectorId (integer), OCPP 2.0.1 uses evseId
    request_body["connectorId"] = db_evse.ocpp_evse_id
    request_body["evseId"] = db_evse.ocpp_evse_id

    debug(" [Stripe] remote start request: %r", json.dumps(request_body))

    # Use OCPP 1.6 endpoint (not 2.0.1 requestStartTransaction)
    citrineos_module = "1.6/evdriver"
    action = "remoteStartTransaction"
    response = ocpp_integration.send_citrineos_message(
        station_id=db_evse.station_id,
        tenant_id=db_evse.tenant_id,
        url_path=f"{citrineos_module}/{action}",
        json_payload=request_body,
    )
    remote_start_stop = RequestStartStopStatusEnumType.REJECTED
    if response.status_code == 200:
        # Core returns [{"success": true}] (list) — handle both list and dict
        resp_json = response.json()
        if isinstance(resp_json, list) and len(resp_json) > 0:
            success = resp_json[0].get("success")
        elif isinstance(resp_json, dict):
            success = resp_json.get("success")
        else:
            success = False
        if success:
            remote_start_stop = RequestStartStopStatusEnumType.ACCEPTED
    db_checkout.remote_request_status = remote_start_stop

    db.add(db_checkout)
    db.commit()
    debug(
        " [Stripe] paymentIntentId: %r, checkoutId: %r, requestStartStatus: %r",
        db_checkout.payment_intent_id,
        db_checkout.id,
        db_checkout.remote_request_status,
    )

    # --- FIX 2: Poll CitrineOS Transactions table for the new transaction ID ---
    # RabbitMQ state=2 events are unreliable for capturing the transactionId.
    # Instead, query the CitrineOS DB directly right after remote start succeeds.
    if remote_start_stop == RequestStartStopStatusEnumType.ACCEPTED:
        try:
            core_engine = create_engine(
                f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}"
                f"@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_DATABASE}"
            )
            # Give the charger up to 10 seconds to respond with StartTransaction
            for attempt in range(5):
                time.sleep(2)
                with core_engine.connect() as conn:
                    result = conn.execute(
                        text(
                            """
                            SELECT "transactionId", "startTime"
                            FROM "Transactions"
                            WHERE "stationId" = :station_id
                              AND "isActive" = true
                              AND "startTime" >= NOW() - INTERVAL '60 seconds'
                            ORDER BY "startTime" DESC
                            LIMIT 1
                            """
                        ),
                        {"station_id": db_evse.station_id},
                    ).fetchone()
                    if result:
                        db_checkout.transaction_id = str(result[0])
                        db_checkout.transaction_start_time = result[1]
                        db.add(db_checkout)
                        db.commit()
                        info(
                            " [Webhook] Linked transactionId=%r to checkout_id=%r",
                            result[0],
                            db_checkout.id,
                        )
                        break
                    else:
                        debug(
                            " [Webhook] Attempt %r: no active transaction found yet for station %r, retrying...",
                            attempt + 1,
                            db_evse.station_id,
                        )
            else:
                warning(
                    " [Webhook] Could not find active transaction for station %r after remote start.",
                    db_evse.station_id,
                )
        except Exception as e:
            exception(" [Webhook] Error while polling for transactionId: %r", e)


async def handle_scan_and_charge(
    db: Session,
    ocpp_integration: OcppIntegration,
    db_checkout: CheckoutModel,
    paymentIntentId: str,
    stationId: str,
    transactionId: str,
):
    ocppTransaction = (
        db.query(Transaction)
        .filter(
            Transaction.stationId == stationId,
            Transaction.transactionId == transactionId,
        )
        .first()
    )
    if ocppTransaction is None:
        debug(" [Stripe] No transaction found for checkout session")
        cancel_payment_intent(paymentIntentId)
        raise HTTPException(
            status_code=404, detail="No transaction found for checkout session"
        )
    if ocppTransaction.isActive is False:
        debug(" [Stripe] Transaction is not active")
        cancel_payment_intent(paymentIntentId)
        raise HTTPException(status_code=404, detail="Transaction is not active")

    authorization = await ocpp_integration.create_authorization(
        f"{Config.OCPP_REMOTESTART_IDTAG_PREFIX}{db_checkout.id}",
        "Central",
        [
            (transactionId, "TransactionId"),
            (paymentIntentId, "PaymentIntentId"),
        ],
    )
    if authorization is None:
        debug(" [Stripe] Unable to create authorization for transaction")
        cancel_payment_intent(paymentIntentId)
        raise HTTPException(
            status_code=404, detail="Unable to create authorization for transaction"
        )

    idToken = authorization["idToken"]
    request_body = {
        "remoteStartId": db_checkout.id,
        "idToken": idToken,
        "idTag": idToken["idToken"],
    }

    if ocppTransaction.evse is not None:
        request_body["evseId"] = ocppTransaction.evse.id

    debug(" [Stripe] remote start request: %r", json.dumps(request_body))

    db_evse = db.query(Evse).filter(Evse.station_id == stationId).first()
    # Use OCPP 1.6 endpoint (not 2.0.1 requestStartTransaction)
    citrineos_module = "1.6/evdriver"
    action = "remoteStartTransaction"
    response = ocpp_integration.send_citrineos_message(
        station_id=stationId,
        tenant_id=db_evse.tenant_id,
        url_path=f"{citrineos_module}/{action}",
        json_payload=request_body,
    )
    remote_start_stop = RequestStartStopStatusEnumType.REJECTED
    if response.status_code == 200:
        # Core returns [{"success": true}] (list) — handle both list and dict
        resp_json = response.json()
        if isinstance(resp_json, list) and len(resp_json) > 0:
            success = resp_json[0].get("success")
        elif isinstance(resp_json, dict):
            success = resp_json.get("success")
        else:
            success = False
        if success:
            remote_start_stop = RequestStartStopStatusEnumType.ACCEPTED
    db_checkout.remote_request_status = remote_start_stop
    db_checkout.payment_intent_id = paymentIntentId
    db.add(db_checkout)
    db.commit()

    citrineos_module = (
        "configuration"  # TODO set up programatic way to resolve module from action
    )
    action = "clearDisplayMessage"
    ocpp_integration.send_citrineos_message(
        station_id=stationId,
        tenant_id=db_evse.tenant_id,
        url_path=f"{citrineos_module}/{action}",
        json_payload={"id": db_checkout.qr_code_message_id},
    )


def cancel_payment_intent(paymentIntendId: str):
    try:
        stripe.PaymentIntent.cancel(paymentIntendId)
    except Exception as e:
        exception(" [Stripe] Error while canceling payment intent: %r", e.__str__())
