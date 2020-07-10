from datetime import datetime
from enum import Enum
from typing import NamedTuple


class DataMeasurementType(Enum):
    confirmed = "confirmed"
    deaths = "deaths"


class CovidDataPoint(NamedTuple):
    """
    Represents an entry for a measurement.
    """

    value: int
    timestamp: datetime
    iso_3: str
    country: str
    state_province: str
    county: str
    lat: float
    long: float
    measurement: DataMeasurementType
