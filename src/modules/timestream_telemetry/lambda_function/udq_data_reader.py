# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Mock UDQ data reader for CookieFactoryV3 demo
# Replaces Timestream for LiveAnalytics (access restricted for new accounts)

import json
import random
import time


def lambda_handler(event, context):
    """Return mock telemetry data for CookieFactory demo."""
    selected_properties = event.get("selectedProperties", [])
    entity_id = event.get("entityId", "unknown")
    component_name = event.get("componentName", "")

    mock_values = []
    now_ms = int(time.time() * 1000)

    # Generate mock time series data (last 10 data points, 1 min intervals)
    for prop in selected_properties:
        values = []
        for i in range(10):
            ts = now_ms - (i * 60000)

            if "RPM" in prop or "rpm" in prop:
                raw_value = round(random.uniform(80.0, 120.0), 2)
                values.append({
                    "timestamp": ts,
                    "value": {"doubleValue": raw_value}
                })
            elif "Temperature" in prop or "temperature" in prop:
                raw_value = round(random.uniform(65.0, 85.0), 2)
                values.append({
                    "timestamp": ts,
                    "value": {"doubleValue": raw_value}
                })
            elif "alarm" in prop.lower() or "status" in prop.lower() or "State" in prop:
                raw_value = random.choice(["NORMAL", "ACTIVE", "ACKNOWLEDGED"])
                values.append({
                    "timestamp": ts,
                    "value": {"stringValue": raw_value}
                })
            else:
                raw_value = round(random.uniform(0.0, 100.0), 2)
                values.append({
                    "timestamp": ts,
                    "value": {"doubleValue": raw_value}
                })

        mock_values.append({
            "entityPropertyReference": {
                "entityId": entity_id,
                "componentName": component_name,
                "propertyName": prop
            },
            "values": values
        })

    return {"propertyValues": mock_values, "nextToken": None}
