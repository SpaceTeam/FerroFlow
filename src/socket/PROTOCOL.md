# Socket Protocol

## Message Format

Messages are sent over TCP. Each message starts with a 2-byte unsigned big-endian length header, followed by the JSON-encoded message body.

Every message body is a JSON object:

```json
{
  "type": "message_type",
  "content": {
    // message-specific data
  },
}
```



## Field References

Fields can be referenced by either their mapped name or their raw name.
Mapped names are unique across all nodes, while raw names are only unique within a node.
Raw names are constructed from the node name and the field name: `<node_name>:<field_name>`.
Seperate from this



The value is either the mapped or the raw value, depending on the `value_type` of the field reference.

Commands that target a field use one of these explicit references:

mapped value reference, resolved by mapped name:
```json
{
    "value_type": "mapped",
    "field_name": "tank_pressure"
}
```

mapped value reference, resolved by raw name:
```json
{
    "value_type": "mapped",
    "field_name": "ECU:tank_pressure"
}
```


Raw value reference, resolved raw name:

```json
{
  "value_type": "raw",
  "field_name": "ECU:tank_pressure"
}
```

Raw value reference, resolved mapped name:

```json
{
  "value_type": "raw",
  "field_name": "tank_pressure"
}
```

## Messages From FerroFlow

### Telemetry

Sent at connection and on request. Contains all current cached telemetry values for all nodes.

```json
{
    "type": "telemetry",
    "content": {
        "timestamp": 1710000000000,
        "nodes": [
            {
                "id": 5,
                "name": "ECU",
                "telemetry": [
                    {
                        "id": 10,
                        "name": "tank_pressure",
                        "raw_name": "ECU:pressure_adc",
                        "mapped_name": "tank_pressure",
                        "raw": 198,
                        "value": 100.0,
                        "unit": "bar",
                        "logical": "High"
                    }
                ]
            }
        ]
    }
}
```

If a field has no mapping, `name` equals `raw_name`, `mapped_name` is `null`, `value` equals `raw`, `unit` is empty, and `logical` is `null`.

### TelemetryDelta

Sent when telemetry values change. Contains values that changed since the previous `telemetry` or `telemetry_delta` message sent on the socket.

```json
{
    "type": "telemetry_delta",
    "content": {
        "prev_timestamp": 1710000000000,
        "timestamp": 1710000000020,
        "nodes": [
            {
                "id": 5,
                "name": "ECU",
                "telemetry": [
                    {
                        "id": 10,
                        "name": "tank_pressure",
                        "raw_name": "ECU:pressure_adc",
                        "mapped_name": "tank_pressure",
                        "raw": 199,
                        "value": 100.5,
                        "unit": "bar",
                        "logical": "High"
                    }
                ]
            }
        ]
    }
}
```

### Nodes

Sent at connection, whenever the registered node list changes, and on request.

```json
{
    "type": "nodes",
    "content": {
        "nodes": [
            {
                "id": 5,
                "name": "ECU",
                "fields": [
                    {
                        "id": 10,
                        "type": "telemetry",
                        "name": "tank_pressure",
                        "raw_name": "ECU:pressure_adc",
                        "mapped_name": "tank_pressure"
                    },
                    {
                        "id": 20,
                        "type": "parameter",
                        "name": "valve_opening",
                        "raw_name": "ECU:valve_raw",
                        "mapped_name": "valve_opening"
                    }
                ]
            }
        ]
    }
}
```

### FieldGetResponse

Sent in response to a `get_field` command once the value is available.

```json
{
    "type": "field_get_response",
    "content": {
        "node_id": 5,
        "node_name": "ECU",
        "raw_name": "ECU:pressure_adc",
        "mapped_name": "tank_pressure",
        "name": "tank_pressure",
        "raw": 198,
        "value": 100.0,
        "unit": "bar",
        "logical": "High"
    }
}
```

## Commands To FerroFlow

### SetParameter

Sets a parameter value. With a mapped reference, FerroFlow inverse-applies the mapping before sending the raw CAN value. With a raw reference, FerroFlow sends the JSON value converted directly to the registered CAN data type.
The type of value set is defined by the `value_type` of the field reference.

```json
{
    "type": "set_parameter",
    "content": {
        "field": {
            "value_type": "mapped",
            "name": "valve_opening"
        },
        "value": 60.0
    }
}
```

```json
{
    "type": "set_parameter",
    "content": {
        "field": {
              "value_type": "raw",
              "name": "valve_opening"
        },
        "value": 100
    }
}
```

### GetField

Requests the current value of a field from a node.
The value is either the raw or the mapped value, depending on the `value_type` of the field reference.

```json
{
    "type": "get_field",
    "content": {
        "field": {
          "value_type": "raw", 
          "name": "valve_opening"
        }
    }
}
```

```json
{
    "type": "get_field",
    "content": {
        "field": {
          "value_type": "mapped",
          "name": "valve_opening"
        }
    }
}
```

### GetNodes

Requests a `nodes` message.

```json
{
    "type": "get_nodes",
    "content": {}
}
```

### GetTelemetry

Requests a full `telemetry` message.

```json
{
    "type": "get_telemetry",
    "content": {}
}
```
