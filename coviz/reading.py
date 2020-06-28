import csv
import re
from abc import ABC
from abc import abstractmethod
from datetime import datetime
from typing import Dict
from typing import Iterator

from influxdb_client import Point

from coviz.types import CovidDataPoint
from coviz.types import DataMeasurementType


class CovidDataReader(ABC):
    @abstractmethod
    def get_data_points(self) -> Iterator[CovidDataPoint]:
        pass


class CSSEDataReader(CovidDataReader):
    def __init__(self, file_name: str, measurement: DataMeasurementType):
        self._file_name = file_name
        self._measurement = measurement

    def get_data_points(self) -> Iterator[CovidDataPoint]:
        """
        Implements the CovidDataReader interface for data at:
        https://github.com/CSSEGISandData/COVID-19
        """

        with open(file=self._file_name) as file:
            data_reader = csv.DictReader(file)
            for row in data_reader:
                data_points = self.parse_row(row, self._measurement)
                for dp in data_points:
                    yield dp

    @staticmethod
    def parse_row(
        row: Dict[str, str], measurement: DataMeasurementType
    ) -> Iterator[CovidDataPoint]:
        iso3 = row.get("iso3") or "UNKNOWN_ISO3"
        country = row.get("Country_Region") or "UNKNOWN_COUNTRY"
        state_province = row.get("Province_State") or "UNKNOWN_STATE"
        lat = row.get("Lat") or "0"
        long = row.get("Long_") or "0"
        county = row.get("Admin2") or "UNKNOWN_COUNTY"

        for key in row.keys():
            # dates are in the form m/d/y e.g. 3/19/20
            if re.match(r"\d{1,2}/\d{1,2}/\d{2}", key):
                date = datetime.strptime(key, "%m/%d/%y")
                value = int(row.get(key) or "0")
                yield CovidDataPoint(
                    value=value,
                    timestamp=date,
                    iso_3=iso3,
                    country=country,
                    county=county,
                    state_province=state_province,
                    lat=float(lat),
                    long=float(long),
                    measurement=measurement,
                )


def convert_data_point_to_influx_db(data_point: CovidDataPoint) -> Point:
    pass
