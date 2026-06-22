# OPC UA Simulation Server

This project provides a small OPC UA simulation server for a simple factory setup with:

- Robot tags
- Conveyor belt tags
- Laser sensor tag

The server is implemented in `server.py` using `asyncua`.

## What The Server Does

When running, the server:

- Starts an OPC UA endpoint at `opc.tcp://localhost:4840`
- Creates a namespace: `http://openindustryproject.github.io/robot0`
- Creates objects under `FactoryFloor`:
  - `Robot`
  - `ConveyorBelt`

### Exposed Variables

Robot:

- `Command` (String)
- `Execute` (Boolean)
- `Done` (Boolean)
- `GripperState` (Boolean)

ConveyorBelt:

- `Running` (Boolean)
- `Speed` (Float)
- `LaserSensor` (Boolean)

### Current Runtime Logic

In the current script logic:

- If `LaserSensor` is `True`, the server sets:
  - `Running = False`
  - `Speed = 0.0`
- Then it waits 5 seconds.

Note: In the current version there is no `else` branch that restores conveyor movement automatically after sensor becomes `False`.

## Requirements

- Python 3.10+
- `asyncua`

Install dependency:

```powershell
pip install asyncua
```

## How To Run

From this folder:

```powershell
python server.py
```

You should see logs including:

- Server started successfully
- Endpoint: `opc.tcp://localhost:4840`

Stop the server with `Ctrl+C`.

## OPC UA Connection

For local connection (same machine), use:

- `opc.tcp://localhost:4840`

## For Connection in Open Indusrty Project simulation

- connect to opc.tcp://localhost:4840 endpoint
- Enable Comms
- Add tags (copy and paste nodeIds to wanted tags in simulated machines)
- for more detailed instructions on connecting check: https://github.com/Open-Industry-Project/Open-Industry-Project#communications
