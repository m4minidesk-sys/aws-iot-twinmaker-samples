# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2021
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import sys
from datetime import datetime

import boto3
from influxdb_client import InfluxDBClient

from udq_utils.udq import SingleEntityReader, MultiEntityReader, IoTTwinMakerDataRow, IoTTwinMakerUdqResponse
from udq_utils.udq_models import IoTTwinMakerUDQEntityRequest, IoTTwinMakerUDQComponentTypeRequest, OrderBy, IoTTwinMakerReference, \
    EntityComponentPropertyRef, ExternalIdPropertyRef

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
#   Sample implementation of an AWS IoT TwinMaker UDQ Connector against Timestream for InfluxDB
#   consists of the EntityReader and IoTTwinMakerDataRow implementations
# ---------------------------------------------------------------------------


def _get_influxdb_token() -> str:
    """Retrieve InfluxDB token from AWS Secrets Manager."""
    secret_arn = os.environ['INFLUXDB_TOKEN']
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_arn)
    secret = response['SecretString']
    try:
        return json.loads(secret)['token']
    except (json.JSONDecodeError, KeyError):
        return secret


class InfluxDbReader(SingleEntityReader, MultiEntityReader):
    """
    The UDQ Connector implementation for Timestream for InfluxDB.
    It supports both single-entity queries and multi-entity queries and contains 2 utility functions to read from InfluxDB
    and convert the results into a IoTTwinMakerUdqResponse object.
    """
    def __init__(self, influx_client: InfluxDBClient, org: str, bucket: str):
        self.influx_client = influx_client
        self.org = org
        self.bucket = bucket
        self.query_api = influx_client.query_api()

    # overrides SingleEntityReader.entity_query abstractmethod
    def entity_query(self, request: IoTTwinMakerUDQEntityRequest) -> IoTTwinMakerUdqResponse:
        """
        This is a entityId.componentName.propertyId type query.
        The entityId and componentName is resolved into the externalId's for this component so we are getting telemetryAssetId and telemetryAssetType passed in.
        We are selecting all entries matching the passed in telemetryAssetType, telemetryAssetId and additional filters.
        """
        LOGGER.info("InfluxDbReader entity_query")

        selected_properties = request.selected_properties
        property_filter = request.property_filters[0] if request.property_filters else None

        telemetry_asset_type = request.udq_context['properties']['telemetryAssetType']['value']['stringValue']
        telemetry_asset_id = request.udq_context['properties']['telemetryAssetId']['value']['stringValue']

        measure_name_filter = " or ".join([f'r["_field"] == "{p}"' for p in selected_properties])
        sort_desc = 'true' if request.order_by == OrderBy.DESCENDING else 'false'

        filter_clause = ""
        if property_filter:
            op = property_filter['operator']
            val = property_filter['value']['stringValue']
            filter_clause = f'  |> filter(fn: (r) => r["_value"] {op} "{val}")\n'

        flux_query = (
            f'from(bucket: "{self.bucket}")\n'
            f'  |> range(start: {request.start_time}, stop: {request.end_time})\n'
            f'  |> filter(fn: (r) => r["TelemetryAssetType"] == "{telemetry_asset_type}")\n'
            f'  |> filter(fn: (r) => r["TelemetryAssetId"] == "{telemetry_asset_id}")\n'
            f'  |> filter(fn: (r) => {measure_name_filter})\n'
            f'{filter_clause}'
            f'  |> sort(columns: ["_time"], desc: {sort_desc})\n'
        )
        if request.max_rows:
            flux_query += f'  |> limit(n: {request.max_rows})\n'

        rows = self._run_influxdb_query(flux_query)
        return self._convert_influxdb_rows_to_udq_response(rows, request.entity_id, request.component_name, telemetry_asset_type)

    # overrides MultiEntityReader.component_type_query abstractmethod
    def component_type_query(self, request: IoTTwinMakerUDQComponentTypeRequest) -> IoTTwinMakerUdqResponse:
        """
        This is a componentTypeId query.
        The componentTypeId is resolved into the (partial) externalId's for this component type so we are getting a telemetryAssetType passed in.
        We are selecting all entries matching the passed in telemetryAssetType and additional filters.
        """
        LOGGER.info("InfluxDbReader component_type_query")

        selected_properties = request.selected_properties
        property_filter = request.property_filters[0] if request.property_filters else None
        telemetry_asset_type = request.udq_context['properties']['telemetryAssetType']['value']['stringValue']

        measure_name_filter = " or ".join([f'r["_field"] == "{p}"' for p in selected_properties])
        sort_desc = 'true' if request.order_by == OrderBy.DESCENDING else 'false'

        filter_clause = ""
        if property_filter:
            op = property_filter['operator']
            val = property_filter['value']['stringValue']
            filter_clause = f'  |> filter(fn: (r) => r["_value"] {op} "{val}")\n'

        flux_query = (
            f'from(bucket: "{self.bucket}")\n'
            f'  |> range(start: {request.start_time}, stop: {request.end_time})\n'
            f'  |> filter(fn: (r) => r["TelemetryAssetType"] == "{telemetry_asset_type}")\n'
            f'  |> filter(fn: (r) => {measure_name_filter})\n'
            f'{filter_clause}'
            f'  |> sort(columns: ["_time"], desc: {sort_desc})\n'
        )
        if request.max_rows:
            flux_query += f'  |> limit(n: {request.max_rows})\n'

        rows = self._run_influxdb_query(flux_query)
        return self._convert_influxdb_rows_to_udq_response(rows, request.entity_id, request.component_name, telemetry_asset_type)

    def _run_influxdb_query(self, flux_query: str) -> list:
        """
        Utility function: handles executing the given Flux query on Timestream for InfluxDB.
        Returns a list of FluxRecord objects.
        """
        LOGGER.info("Flux query: %s", flux_query)
        try:
            tables = self.query_api.query(flux_query, org=self.org)
            records = []
            for table in tables:
                records.extend(table.records)
            return records
        except Exception as err:
            LOGGER.error("Exception while running InfluxDB query: %s", err)
            raise err

    @staticmethod
    def _convert_influxdb_rows_to_udq_response(records, entity_id, component_name, telemetry_asset_type) -> IoTTwinMakerUdqResponse:
        """
        Utility function: handles converting InfluxDB FluxRecord objects into a IoTTwinMakerUdqResponse object.
        """
        LOGGER.info("InfluxDB query returned %d records", len(records))
        result_rows = []
        for record in records:
            result_rows.append(InfluxDbDataRow(record, entity_id, component_name, telemetry_asset_type))
        return IoTTwinMakerUdqResponse(result_rows, None)


class InfluxDbDataRow(IoTTwinMakerDataRow):
    """
    The AWS IoT TwinMaker data row implementation for InfluxDB data.

    It supports the IoTTwinMakerDataRow interface to:
    - calculate the IoTTwinMakerReference ("entityPropertyReference") for an InfluxDB record
    - extract the timestamp from an InfluxDB record
    - extract the value from an InfluxDB record
    """

    def __init__(self, influxdb_record, entity_id=None, component_name=None, telemetry_asset_type=None):
        self._record = influxdb_record
        self._entity_id = entity_id
        self._component_name = component_name
        self._telemetry_asset_type = telemetry_asset_type

    # overrides IoTTwinMakerDataRow.get_iottwinmaker_reference abstractmethod
    def get_iottwinmaker_reference(self) -> IoTTwinMakerReference:
        """
        Calculates the IoTTwinMakerReference for an InfluxDB record.
        For single-entity queries, uses entity_id and component_name.
        For multi-entity queries, returns the external_id property stored in InfluxDB.
        """
        property_name = self._record.get_field()
        if self._entity_id and self._component_name:
            return IoTTwinMakerReference(ecp=EntityComponentPropertyRef(self._entity_id, self._component_name, property_name))
        else:
            telemetry_asset_id = self._record.values.get('TelemetryAssetId', '')
            external_id_property = {
                'alarm_key' if self._telemetry_asset_type == 'Alarm' else 'telemetryAssetId': telemetry_asset_id,
            }
            return IoTTwinMakerReference(eip=ExternalIdPropertyRef(external_id_property, property_name))

    # overrides IoTTwinMakerDataRow.get_iso8601_timestamp abstractmethod
    def get_iso8601_timestamp(self) -> str:
        """
        Extracts the timestamp from an InfluxDB record and returns in ISO8601 format.
        """
        ts = self._record.get_time()
        return ts.strftime('%Y-%m-%dT%H:%M:%S.%f000Z')

    # overrides IoTTwinMakerDataRow.get_value abstractmethod
    def get_value(self):
        """
        Extracts the value from an InfluxDB record.
        Returns native Python type (str or float).
        """
        value = self._record.get_value()
        if isinstance(value, float):
            return value
        return str(value)


# Initialize InfluxDB client from environment variables
if os.environ.get("AWS_EXECUTION_ENV") is not None:
    INFLUXDB_ENDPOINT = os.environ['INFLUXDB_ENDPOINT']
    INFLUXDB_ORG = os.environ['INFLUXDB_ORG']
    INFLUXDB_BUCKET = os.environ['INFLUXDB_BUCKET']
    INFLUXDB_TOKEN = _get_influxdb_token()
else:
    LOGGER.addHandler(logging.StreamHandler(sys.stdout))
    INFLUXDB_ENDPOINT = None
    INFLUXDB_ORG = None
    INFLUXDB_BUCKET = None
    INFLUXDB_TOKEN = None

INFLUX_CLIENT = InfluxDBClient(url=INFLUXDB_ENDPOINT, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) if INFLUXDB_ENDPOINT else None
INFLUXDB_UDQ_READER = InfluxDbReader(INFLUX_CLIENT, INFLUXDB_ORG, INFLUXDB_BUCKET) if INFLUX_CLIENT else None


# Main Lambda invocation entry point, use the InfluxDbReader to process events
# noinspection PyUnusedLocal
def lambda_handler(event, context):
    LOGGER.info('Event: %s', event)
    result = INFLUXDB_UDQ_READER.process_query(event)
    LOGGER.info("result:")
    LOGGER.info(result)
    return result
