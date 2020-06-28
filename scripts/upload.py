import argparse
from datetime import datetime
from typing import Optional

from coviz.reading import CSSEDataReader
from coviz.types import DataMeasurementType
from coviz.writing import InfluxDBDataWriter


def main(
    file_name: str,
    measurement: str,
    url: str,
    api_key: str,
    org: str,
    bucket: str,
    start: Optional[str],
):

    reader = CSSEDataReader(
        file_name=file_name, measurement=DataMeasurementType(measurement),
    )

    writer = InfluxDBDataWriter(url=url, token=api_key, org=org, bucket=bucket,)

    start_date: Optional[datetime] = None

    if start is not None:
        start_date = datetime.strptime(start, "%Y-%M-%d")

    for data_point in reader.get_data_points():
        if start_date:
            if data_point.timestamp < start_date:
                continue
        writer.write_data(data_point)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Uploads COVID data to an influx-db instance",
    )

    parser.add_argument(
        "-f",
        "--file",
        dest="file_name",
        type=str,
        required=True,
        help="CSV file with the data",
    )

    parser.add_argument(
        "-m",
        "--measurement",
        dest="measurement",
        type=str,
        choices=["confirmed", "deaths"],
        default="confirmed",
        required=False,
        help="Specific measurement this file represents",
    )

    parser.add_argument(
        "-u",
        "--url",
        dest="url",
        type=str,
        required=True,
        help="InfluxDB url to write data to",
    )

    parser.add_argument(
        "-k",
        "--api_key",
        dest="api_key",
        type=str,
        required=True,
        help="InfluxDB API key",
    )

    parser.add_argument(
        "-o", "--org", dest="org", type=str, required=True, help="InfluxDB org",
    )

    parser.add_argument(
        "-b",
        "--bucket",
        dest="bucket",
        type=str,
        required=True,
        help="InfluxDB data bucket",
    )

    parser.add_argument(
        "-s",
        "--start",
        dest="start",
        type=str,
        required=False,
        help="Optional inclusive start data for data to upload. The expected "
        "format is `YYYY-MM-DD` if not set, all data will be uploaded",
    )

    return parser.parse_args()


if __name__ == "__main__":
    parsed = parse_args()
    main(**vars(parsed))
