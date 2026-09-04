# OPC UA Simulation Server

This repository contains a Python gateway for a two-station factory simulation. It exposes robot and conveyor state through OPC UA, receives operation requests over MQTT, and publishes AAS-oriented telemetry and correlated operation replies.

The implementation is in `server.py` and uses `asyncua` and `aiomqtt`.

## Requirements

- Python 3.10 or newer
- An MQTT broker listening on `localhost:1883`
- Python packages `asyncua` and `aiomqtt`

Create an environment and install the Python dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install asyncua aiomqtt
```

The repository does not start an MQTT broker. Start one separately before running the server.

## Running the server

```powershell
python server.py
```

By default the process creates `Station_01` and `Station_02`, connects to MQTT at `localhost:1883`, and listens for OPC UA clients on port `4840`. Stop it with `Ctrl+C`.

The following environment variables are supported:

| Variable | Default | Purpose |
| --- | --- | --- |
| `STATION_IDS` | `Station_01,Station_02` | Comma-separated OPC UA station objects to create. |
| `OPCUA_SESSION_TIMEOUT_MS` | `86400000` | Minimum and maximum OPC UA session timeout, in milliseconds. |

The MQTT host and OPC UA port are currently fixed in `server.py`.

## OPC UA interface

The server binds to `opc.tcp://0.0.0.0:4840`. A client on the same machine should connect to:

```text
opc.tcp://localhost:4840
```

Only the `NoSecurity` policy is enabled. The namespace URI is:

```text
http://openindustryproject.github.io/robot0
```

Each configured station has this structure below `Objects/FactoryFloor`:

```text
Station_XX
├── Robot
│   ├── Command       Int16    (initial value: 1)
│   ├── Execute       Boolean  (initial value: false)
│   ├── Done          Boolean  (initial value: false)
│   ├── GripperState  Boolean  (initial value: false)
│   └── FaultActive   Boolean  (initial value: false)
└── ConveyorBelt
    ├── Running       Boolean  (initial value: true)
    ├── Speed         Float    (initial value: 1.0)
    └── LaserSensor   Float    (initial value: 0.0)
```

All variables are writable so the external simulation can update sensor and robot feedback. In the intended handshake, the gateway drives `Command`, `Execute`, and `GripperState`; the robot simulation drives `Done` and `FaultActive`; and the conveyor simulation drives `LaserSensor`.

Robot operations are rejected until that robot reports `Done=true` while `Execute=false`.

## MQTT input

The server subscribes to both operation patterns:

| Topic pattern | Routing behavior |
| --- | --- |
| `simulation/<StationId>/operations/<operation>` | Sends the request to the station and uses its local robot for robot operations. |
| `simulation/robots/<RobotId>/operations/<operation>` | Selects a specific robot as executor. For `moveBox`, the source asset in `params.SourcePosition` selects the destination station. Only robot operations are accepted. |

The fixed topic segments (`simulation`, `robots`, and `operations`) are case-sensitive. Station IDs, robot IDs, and operation names are matched case-insensitively. `SourcePosition` and `TargetPosition` route values are case-sensitive.

### Recommended request envelope

Use a correlated JSON envelope. Keep its `operation` value consistent with the operation in the topic:

```json
{
  "requestId": "request-123",
  "runId": "experiment-01",
  "stationId": "Station_01",
  "robotId": "Robot_02",
  "operation": "moveBox",
  "params": {
    "SourcePosition": "urn:agent-aas:asset-instance:conveyor01",
    "TargetPosition": "urn:agent-aas:entity:oip-factory01:pallet01"
  }
}
```

`requestId` enables acknowledgement messages. `runId` is optional and is used to group latency samples in `OIP_server_logs.csv`. `robotId` is optional on a station-routed request; on a robot-routed request, if supplied, it must match the robot in the topic.

Every `moveBox` request must use the exact source-asset and target-entity URNs configured in the route matrix below. For a robot-routed `moveBox`, the server also uses `SourcePosition` to find the station whose pending box detection will be consumed. `stationId` is not used to select that station, but, if supplied, it must match the station resolved from `SourcePosition`. For a robot-routed `moveToHome`, the destination is the selected robot's own station and any supplied `stationId` must match it.

### Supported operations

| Operation | Parameters | Behavior |
| --- | --- | --- |
| `conveyorRunning` | `{"value": true}` or `{"running": true}` | Sets the requested running state. A detected box keeps the applied state false. |
| `conveyorSpeed` | `{"value": 1.0}` or `{"speed": 1.0}` | Sets a non-negative target speed. A detected box keeps the applied speed at `0.0`. |
| `moveBox` | `{"SourcePosition": "<source-asset-URN>", "TargetPosition": "<target-entity-URN>"}` | Consumes one confirmed, unconsumed box detection at the destination station and executes the configured pick-and-place sequence. |
| `moveToHome` | `{"value": true}` or `{"move": true}` | Validates a boolean-like value and sends robot command `0`. The current implementation also executes the command when that value is false. |

For backwards compatibility, station-routed conveyor requests can also use scalar payloads such as `true` or `1.0`, or a parameter object without the outer request envelope. Scalar requests do not produce replies because they have no `requestId`.

The built-in `moveBox` route matrix is:

| Executor | Destination | Source | Target |
| --- | --- | --- | --- |
| `Robot_01` | `Station_01` | `urn:agent-aas:asset-instance:conveyor01` | `urn:agent-aas:entity:oip-factory01:pallet01` |
| `Robot_02` | `Station_01` | `urn:agent-aas:asset-instance:conveyor01` | `urn:agent-aas:entity:oip-factory01:pallet01` |
| `Robot_02` | `Station_02` | `urn:agent-aas:asset-instance:conveyor02` | `urn:agent-aas:entity:oip-factory01:pallet01` |

The destination shown above is resolved from the source asset. Other robot, source, or target combinations fail unless a route is added to `_build_robot_sequences()`. A new source asset must also be registered in `controllers_by_asset` so robot-routed requests can resolve its station.

Example using Mosquitto to assign `Robot_02` to a box at `Station_01`:

```powershell
mosquitto_pub -h localhost -t "simulation/robots/Robot_02/operations/moveBox" -m '{"requestId":"request-123","stationId":"Station_01","robotId":"Robot_02","operation":"moveBox","params":{"SourcePosition":"urn:agent-aas:asset-instance:conveyor01","TargetPosition":"urn:agent-aas:entity:oip-factory01:pallet01"}}'
```

## MQTT output

All messages use QoS 1.

### Telemetry

State changes are published, without retain, to:

```text
oip/telemetry
```

Payload format:

```json
{
  "assetId": "urn:agent-aas:asset-instance:conveyor01",
  "semanticId": "urn:agent-aas:semantics:IsRunning:1",
  "value": true,
  "eventId": "urn:agent-aas:asset-instance:conveyor01:<server-instance-id>:00000001"
}
```

The server emits these semantic values:

| Asset | Semantic ID | Value |
| --- | --- | --- |
| Conveyor | `urn:agent-aas:semantics:IsRunning:1` | Boolean running state |
| Conveyor | `urn:agent-aas:semantics:ActualConveyorSpeed:1` | Current numeric speed |
| Conveyor | `urn:agent-aas:semantics:WorkpiecePresent:1` | Boolean box presence |
| Robot | `urn:agent-aas:semantics:IsMoving:1` | Boolean execution state |
| Robot | `urn:agent-aas:semantics:FaultActive:1` | Boolean fault state |

Asset IDs are derived from the generated equipment ID. For example, `Station_01` uses `urn:agent-aas:asset-instance:robot01` and `urn:agent-aas:asset-instance:conveyor01`.

### Operation replies

Requests with a `requestId` receive acknowledgements on:

```text
simulation/<StationId>/replies/<operation>
```

A valid operation publishes `started` and then `completed`. Validation errors, unsupported routes, robot readiness failures, and execution errors publish `failed`, with an `error` field. Robot-operation replies also identify the selected `robotId`.

Some robot-routed requests can be rejected before a destination station is known—for example, an unknown robot, a topic/payload robot mismatch, or a missing or unknown `SourcePosition`. These failures are logged but cannot be published to a station reply topic. Once the source station has been resolved, failures (including a conflicting `stationId`) are published on that station's reply topic.

```json
{
  "requestId": "request-123",
  "stationId": "Station_01",
  "robotId": "Robot_02",
  "operation": "moveBox",
  "status": "completed",
  "timestamp": 1788523200.0
}
```

Replies are not retained.

### Server availability

The retained `simulation/server/status` message contains `online`, `serverInstanceId`, and a Unix `timestamp`. The server publishes `online=true` after connecting. A last will and the normal shutdown path publish `online=false`.

## Box detection and robot behavior

The laser sensor is sampled every `0.05` seconds. A box is present when its distance is strictly between `0.01` and `0.5` for two consecutive samples. On a confirmed detection, the server:

1. Creates one pending pick token.
2. Stops the conveyor and applies speed `0.0`.
3. Publishes the corresponding conveyor telemetry changes.
4. Records detection time `t0` for latency measurement.

The sensor must remain outside the detection range for `0.25` seconds before the box state clears and detection is re-armed. The requested conveyor running state and speed are remembered and restored when it is safe to restart.

During a robot command, the server waits for this `Done` handshake:

1. `Done=true` before issuing the command.
2. `Done=false` within 5 seconds after the rising edge on `Execute`.
3. `Done=true` again within 30 seconds to signal completion.

Robot operations using the same executor are serialized by that robot's lock, including when `Robot_02` services both stations.

## Latency log

For `moveBox`, a confirmed box detection supplies `t0` and operation invocation supplies `t4`. When both are available, the server appends a row to `OIP_server_logs.csv`:

```text
request_id,run_id,station_id,sample_in_run,t0_unix,t4_unix,status
```

The sample counter resets when `runId` changes. At startup, an existing CSV must have exactly this header; otherwise the server raises an error and asks you to move or rename the old log.

## Tests

Run the unit tests with:

```powershell
python -m unittest discover -s tests -v
```
