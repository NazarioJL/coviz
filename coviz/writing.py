from abc import ABC
from abc import abstractmethod

from influxdb_client import InfluxDBClient
from influxdb_client import Point

from coviz.types import CovidDataPoint


class CovidDataWriter(ABC):
    @abstractmethod
    def write_data(self, data_point: CovidDataPoint) -> None:
        pass


class InfluxDBDataWriter(CovidDataWriter):
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self._org = org
        self._bucket = bucket
        self._client = InfluxDBClient(url=url, token=token)
        self._write_api = self._client.write_api()

    def write_data(self, data_point: CovidDataPoint) -> None:
        self._write_api.write(
            bucket=self._bucket,
            org=self._org,
            record=self._convert_datapoint(data_point=data_point),
        )

    @staticmethod
    def _convert_datapoint(data_point: CovidDataPoint) -> Point:
        point = (
            Point(data_point.measurement.value)
            .tag("iso3", data_point.iso_3)
            .tag("country", data_point.country)
            .tag("state_province", data_point.state_province)
            .tag("county", data_point.county)
            .tag("lat", data_point.lat)
            .tag("long", data_point.long)
            .field("count", data_point.value)
            .time(data_point.timestamp)
        )
        return point

    def __del__(self):
        self._client.close()
