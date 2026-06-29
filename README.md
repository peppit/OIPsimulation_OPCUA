# OPC UA Simulation Server

This project provides a small OPC UA simulation server for a simple factory setup with MQTT-based control.

The server is implemented in `server.py` using `asyncua` and `aiomqtt`.

## Current Workflow

The server now works in two directions:

1. It listens for MQTT operation messages from the AAS.
2. It writes the requested values into OPC UA nodes.
3. It also publishes back MQTT status updates for running state, speed, and box detection.

### MQTT Input Topics

The server subscribes to:

`simulation/+/operations/+`

Expected operation topics:

- `simulation/Station_01/operations/conveyorRunning`
- `simulation/Station_01/operations/conveyorSpeed`

Example payloads:

```json
true
```

```json
{"value": true}
```

```json
1.0
```

```json
{"value": 1.0}
```

### MQTT Output Topics

The server publishes status on:

- `simulation/<StationId>/isRunning`
- `simulation/<StationId>/currentSpeed`
- `simulation/<StationId>/boxDetected`

### OPC UA Structure

The server starts an OPC UA endpoint at:

`opc.tcp://localhost:4840`

It creates a namespace:

`http://openindustryproject.github.io/robot0`

It creates objects under `FactoryFloor`:

- `Robot`
- `ConveyorBelt`

### Exposed Variables

Robot:

- `Command`
- `Execute`
- `Done`
- `GripperState`

ConveyorBelt:

- `Running`
- `Speed`
- `LaserSensor`
- `PositionX`
- `PositionY`
- `PositionZ`

## Current Runtime Behavior

The server has a monitoring loop for the laser sensor.

When the laser sensor value is between `0.01` and `0.5`:

- `Running` is written to `False`
- `Speed` is written to `0.0`
- a `boxDetected` MQTT message is published

When no box is present, the server keeps publishing box state changes and continues listening for MQTT operations.

## Requirements

- Python 3.10+
- `asyncua`
- `aiomqtt`

Install dependencies:

```powershell
pip install asyncua aiomqtt
```

## How To Run

From this folder:

```powershell
python server.py
```

You should see logs including:

- Initialized and mapped nodes for Station_01
- Unified OPC UA + MQTT Gateway Environment Online
- MQTT operation listener subscribed to `simulation/+/operations/+`

Stop the server with `Ctrl+C`.

## How To Use With AAS

1. Publish an operation message to the MQTT broker.
2. The server receives the message.
3. The server writes the value to the matching OPC UA node.
4. The server publishes back the current MQTT state.

## OPC UA Connection

For local connection, use:

`opc.tcp://localhost:4840`

## Open Industry Project Setup

1. Connect to `opc.tcp://localhost:4840`.
2. Enable Comms.
3. Add the node IDs to the desired tags.
4. See the Open Industry Project communications guide for more details:

https://github.com/Open-Industry-Project/Open-Industry-Project#communications
