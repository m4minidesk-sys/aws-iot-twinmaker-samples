# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Mock UDQ data reader - replaces Timestream for LiveAnalytics
# (access restricted for new accounts; temporary until InfluxDB migration)

import json
import random
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Property definitions matching cookiefactoryv3 data.csv schema
DOUBLE_PROPERTIES = {
    "Bad",
    "Good",
    "Total",
    "Bad_Parts_1Min",
    "Blocked_Time_1Min",
    "Down_Time_1Min",
    "Temperature",
    "RPM",
    "tankVolume1",
}

STRING_PROPERTIES = {
    "Alarm_State",
    "Alarm_Text",
    "alarm_status",
    "Blocked",
    "Down",
    "Starved",
    "State",
}

MOCK_VALUES = {
    "Alarm_State":         ["NORMAL", "NORMAL", "NORMAL", "ACTIVE", "ACKNOWLEDGED"],
    "alarm_status":        ["NORMAL", "NORMAL", "ACTIVE"],
    "Alarm_Text":          ["", "High Temperature", "RPM out of range"],
    "Blocked":             ["False", "False", "True"],
    "Down":                ["False", "False", "True"],
    "Starved":             ["False", "False", "True"],
    "State":               ["Running", "Running", "Stopped", "Idle"],
    "Bad":                 list(range(0, 20)),
    "Good":                list(range(50, 130)),
    "Total":               list(range(60, 150)),
    "Bad_Parts_1Min":      [0.0, 0.0, 0.0, 5.0, 10.0, 15.0, 20.0],
    "Blocked_Time_1Min":   [0.0, 0.0, 5.0, 10.0, 20.0, 40.0, 60.0],
    "Down_Time_1Min":      [0.0, 0.0, 5.0, 10.0, 25.0, 60.0],
    "Temperature":         [round(x * 0.5, 1) for x in range(130, 170)],  # 65.0~85.0
    "RPM":                 [round(x * 0.5, 1) for x in range(160, 240)],  # 80.0~120.0
    "tankVolume1":         [round(x * 0.1, 2) for x in range(100, 500)],
}


def _make_value(prop_name: str) -> dict:
    if prop_name in STRING_PROPERTIES:
        candidates = MOCK_VALUES.get(prop_name, ["NORMAL"])
        return {"stringValue": random.choice(candidates)}
    else:
        candidates = MOCK_VALUES.get(prop_name, [0.0])
        raw = random.choice(candidates)
        return {"doubleValue": float(raw)}


def lambda_handler(event, context):
    """
    Mock UDQ handler. Supports both single-entity (entityId + componentName)
    and multi-entity (componentTypeId) query patterns used by IoT TwinMaker.
    """
    logger.info("Mock UDQ invoked: %s", json.dumps(event))

    selected_properties = event.get("selectedProperties", [])
    entity_id = event.get("entityId")
    component_name = event.get("componentName", "")

    # Determine time range
    now_ms = int(time.time() * 1000)
    num_points = 10

    property_values = []

    for prop in selected_properties:
        values = []
        for i in range(num_points):
            ts = now_ms - (i * 60_000)  # 1-min intervals going back
            values.append({
                "timestamp": ts,
                "value": _make_value(prop)
            })

        if entity_id:
            # Single-entity query: entityId + componentName pattern
            property_values.append({
                "entityPropertyReference": {
                    "entityId": entity_id,
                    "componentName": component_name,
                    "propertyName": prop,
                },
                "values": values,
            })
        else:
            # Multi-entity query: use externalId pattern
            asset_id = event.get("nextToken", f"mock-asset-{prop}")
            ref = {
                "externalIdProperty": {"telemetryAssetId": asset_id},
                "propertyName": prop,
            }
            property_values.append({
                "entityPropertyReference": ref,
                "values": values,
            })

    return {
        "propertyValues": property_values,
        "nextToken": None,
    }
