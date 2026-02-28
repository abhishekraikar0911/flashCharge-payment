from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, model_validator


class ConnectorStatusEnumType(str, Enum):
    Available = "Available"
    Occupied = "Occupied"
    Reserved = "Reserved"
    Unavailable = "Unavailable"
    Faulted = "Faulted"


# Maps OCPP 1.6 status values to OCPP 2.0.1 ConnectorStatusEnumType
_OCPP16_STATUS_MAP = {
    "Available": ConnectorStatusEnumType.Available,
    "Preparing": ConnectorStatusEnumType.Occupied,
    "Charging": ConnectorStatusEnumType.Occupied,
    "SuspendedEVSE": ConnectorStatusEnumType.Occupied,
    "SuspendedEV": ConnectorStatusEnumType.Occupied,
    "Finishing": ConnectorStatusEnumType.Occupied,
    "Reserved": ConnectorStatusEnumType.Reserved,
    "Unavailable": ConnectorStatusEnumType.Unavailable,
    "Faulted": ConnectorStatusEnumType.Faulted,
}


class StatusNotificationRequest(BaseModel):
    timestamp: datetime
    # OCPP 2.0.1 fields (required by the service logic)
    evseId: Optional[int] = None
    connectorStatus: Optional[ConnectorStatusEnumType] = None
    connectorId: Optional[int] = None  # OCPP 1.6 alias

    @model_validator(mode="before")
    @classmethod
    def handle_ocpp16_fields(cls, values):
        """Map OCPP 1.6 StatusNotification fields to OCPP 2.0.1 equivalents."""
        # Map connectorId → evseId
        if values.get("evseId") is None and values.get("connectorId") is not None:
            values["evseId"] = values["connectorId"]
        # Map OCPP 1.6 'status' → connectorStatus
        if values.get("connectorStatus") is None and values.get("status") is not None:
            mapped = _OCPP16_STATUS_MAP.get(values["status"], ConnectorStatusEnumType.Unavailable)
            values["connectorStatus"] = mapped
        return values

