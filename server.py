import asyncio
import csv
import logging
import json
import os
import sys
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional
from asyncua import Server, ua
from asyncua.common.callback import CallbackType
from aiomqtt import Client as MqttClient, MqttError, Will

logging.basicConfig(level=logging.INFO)
logging.getLogger("asyncua.server.address_space").setLevel(logging.WARNING)
logging.getLogger("asyncua.server.standard_address_space").setLevel(logging.WARNING)
logging.getLogger("asyncua.server.uaprocessor").setLevel(logging.WARNING)

OperationHandler = Callable[[Dict[str, Any], Dict[str, Any]], Awaitable[None]]


def log_failed_opcua_writes(event: Any, _service: Any) -> None:
    """Log the NodeId, value, and status for writes rejected by the OPC UA server."""
    params = getattr(event, "request_params", None)
    results = getattr(event, "response_params", None)
    writes = getattr(params, "NodesToWrite", None)
    if not writes or not results:
        return

    for write, status in zip(writes, results):
        if status.is_good():
            continue

        data_value = getattr(write, "Value", None)
        variant = getattr(data_value, "Value", None)
        logging.error(
            "OPC UA WRITE REJECTED nodeId=%s attributeId=%s "
            "value=%r variantType=%s status=%s",
            write.NodeId,
            write.AttributeId,
            getattr(variant, "Value", None),
            getattr(variant, "VariantType", None),
            status,
        )

class StationOperationDispatcher:
    """
    Config-driven operation dispatcher for station controllers.

    This class is designed to be mixed into the station controller class.
    Required members on self:
    - station_id
    - cmd_node, exec_node, done_node, gripper_node, conveyor_running,
      conveyor_speed
    - target_running, target_speed
    - publish_conveyor_running(), publish_conveyor_speed(), publish_robot_moving()
    - _coerce_bool(), _coerce_float()
    - operation_lock
    """

    ROBOT_READY_TIMEOUT_SECONDS = 5.0
    ROBOT_START_TIMEOUT_SECONDS = 5.0
    ROBOT_MOTION_TIMEOUT_SECONDS = 30.0
    ROBOT_STATUS_POLL_SECONDS = 0.05
    EXECUTE_RESET_SECONDS = 0.1
    ROBOT_OPERATIONS = {"movebox", "movetohome"}
    BOX_PRESENT_CONFIRM_SAMPLES = 2
    BOX_CLEAR_DEBOUNCE_SECONDS = 0.25

    def _build_operation_handlers(self) -> Dict[str, OperationHandler]:
        return {
            "conveyorrunning": self._op_conveyor_running,
            "conveyorspeed": self._op_conveyor_speed,
            "movebox": self._op_move_box,
            "movetohome": self._op_move_to_home,
        }

    def _build_robot_sequences(self) -> Dict[str, Any]:
        station_01_sequence = [
            {"action": "execute_command", "cmd": 1},
            {"action": "set_gripper", "value": True},
            {"action": "sleep", "seconds": 1.0},
            {"action": "execute_command", "cmd": 2},
            {"action": "execute_command", "cmd": 3},
            {"action": "execute_command", "cmd": 4},
            {"action": "set_gripper", "value": False},
            {"action": "sleep", "seconds": 1.5},
            {"action": "execute_command", "cmd": 3},
        ]

        station_02_sequence = [
            {"action": "execute_command", "cmd": 5},
            {"action": "set_gripper", "value": True},
            {"action": "sleep", "seconds": 1.0},
            {"action": "execute_command", "cmd": 6},
            {"action": "execute_command", "cmd": 3},
            {"action": "execute_command", "cmd": 4},
            {"action": "set_gripper", "value": False},
            {"action": "sleep", "seconds": 1.5},
            {"action": "execute_command", "cmd": 3},
        ]

        routes_by_robot = {
            "robot_01": {
                ("station_01", "Conveyor1", "Pallet1"):
                    station_01_sequence,
            },
            "robot_02": {
                ("station_01", "Conveyor1", "Pallet1"):
                    station_01_sequence,
                ("station_02", "Conveyor2", "Pallet1"):
                    station_02_sequence,
            },
        }

        return {
            "moveBox": routes_by_robot.get(self.robot_id.lower(), {}),
            "moveToHome": [
                {"action": "execute_command", "cmd": 0},
            ],
        }

    async def dispatch_operation(self, operation_name: str, payload: Any, selected_executor: Optional["ProductionLineController"] = None) -> None:
        operation_name, payload_envelope = self._normalize_operation_message(operation_name, payload)
        request_id = payload_envelope.get("requestId")
        key = operation_name.strip().lower()
        executor = selected_executor or self
        robot_id = executor.robot_id if key in self.ROBOT_OPERATIONS else None

        station_from_payload = payload_envelope.get("stationId")
        if (
            station_from_payload
            and str(station_from_payload).lower() != self.station_id.lower()
        ):
            logging.warning(
                "[%s] Ignoring operation for different station '%s'",
                self.station_id,
                station_from_payload,
            )
            await self.publish_operation_status(
                operation_name,
                request_id,
                "failed",
                f"Operation addressed to station '{station_from_payload}'",
                robot_id=robot_id,
            )
            return

        handler = self.operation_handlers.get(key)

        if handler is None:
            logging.warning(
                "[%s] Unknown operation '%s'",
                self.station_id,
                operation_name,
            )
            await self.publish_operation_status(
                operation_name,
                request_id,
                "failed",
                f"Unknown operation '{operation_name}'",
                robot_id=robot_id,
            )
            return
        
        if key in self.ROBOT_OPERATIONS and not executor.robot_ready.is_set():
            await self.publish_operation_status(
                operation_name,
                request_id,
                "failed",
                f"Selected robot '{executor.robot_id}' is not ready",
                robot_id=executor.robot_id,
            )
            return

        payload_envelope["_executor"] = executor
        await self.publish_operation_status(
            operation_name,
            request_id,
            "started",
            robot_id=robot_id,
        )

        try:
            await handler(payload_envelope, payload_envelope.get("params", {}))
        except asyncio.CancelledError:
            await self.publish_operation_status(
                operation_name,
                request_id,
                "failed",
                "Operation cancelled",
                robot_id=robot_id,
            )
            raise
        except Exception as exc:
            logging.exception("[%s] Operation '%s' failed", self.station_id, operation_name)
            await self.publish_operation_status(
                operation_name,
                request_id,
                "failed",
                str(exc),
                robot_id=robot_id,
            )
            return

        await self.publish_operation_status(
            operation_name,
            request_id,
            "completed",
            robot_id=robot_id,
        )

    def _normalize_operation_message(self, operation_name: str, payload: Any) -> tuple[str, Dict[str, Any]]:
        # New contract from operation-service:
        # {
        #   "requestId": "...",
        #   "stationId": "Station_01",
        #   "operation": "moveBox",
        #   "params": {...}
        # }
        if isinstance(payload, dict):
            op_from_payload = payload.get("operation")
            params = payload.get("params")
            if isinstance(op_from_payload, str) and isinstance(params, dict):
                envelope = {
                    "requestId": payload.get("requestId"),
                    "runId": payload.get("runId"),
                    "stationId": payload.get("stationId", self.station_id),
                    "robotId": payload.get("robotId"),
                    "operation": op_from_payload,
                    "params": params,
                }
                return op_from_payload, envelope

            # Legacy single-operation payloads.
            envelope = {
                "requestId": payload.get("requestId"),
                "runId": payload.get("runId"),
                "stationId": self.station_id,
                "robotId": payload.get("robotId"),
                "operation": operation_name,
                "params": payload,
            }
            return operation_name, envelope

        return operation_name, {
            "requestId": None,
            "runId": None,
            "stationId": self.station_id,
            "robotId": None,
            "operation": operation_name,
            "params": {"value": payload},
        }

    async def _op_conveyor_running(self, _envelope: Dict[str, Any], params: Dict[str, Any]) -> None:
        value = params.get("value", params.get("running"))
        running = self._coerce_bool(value)
        if running is None:
            raise ValueError(f"Invalid conveyorRunning payload: {params}")

        async with self.conveyor_lock:
            self.target_running = running
            applied_running = running and not self.box_is_present
            await self.conveyor_running.write_value(applied_running)
            await self.publish_conveyor_running(applied_running)
        logging.info("[%s] Applied operation conveyorRunning=%s", self.station_id, applied_running)

    async def _op_conveyor_speed(self, _envelope: Dict[str, Any], params: Dict[str, Any]) -> None:
        value = params.get("value", params.get("speed"))
        speed = self._coerce_float(value)
        if speed is None:
            raise ValueError(f"Invalid conveyorSpeed payload: {params}")
        if speed < 0.0:
            raise ValueError(f"Negative conveyorSpeed is invalid: {speed}")

        async with self.conveyor_lock:
            self.target_speed = speed
            applied_speed = 0.0 if self.box_is_present else speed
            await self.conveyor_speed.write_value(ua.Variant(float(applied_speed), ua.VariantType.Float))
            await self.publish_conveyor_speed(float(applied_speed))
        logging.info("[%s] Applied operation conveyorSpeed=%s", self.station_id, applied_speed)

    async def _op_move_box(self, envelope: Dict[str, Any], params: Dict[str, Any]) -> None:
        destination = self
        executor = envelope.get("_executor") or self
        source_position = params.get("SourcePosition")
        target_position = params.get("TargetPosition")
        if not isinstance(source_position, str) or not source_position.strip():
            raise ValueError(f"moveBox requires SourcePosition, got: {params}")
        if not isinstance(target_position, str) or not target_position.strip():
            raise ValueError(f"moveBox requires TargetPosition, got: {params}")

        source_position = source_position.strip()
        target_position = target_position.strip()
        move_box_routes = executor.robot_sequences.get("moveBox", {})
        if not isinstance(move_box_routes, dict):
            raise ValueError("moveBox route configuration is invalid")

        route_key = (destination.station_id.strip().lower(), source_position, target_position)
        sequence = move_box_routes.get(route_key)

        if sequence is None:
            raise ValueError(
                "No moveBox sequence configured for "
                f"station={destination.station_id}, robot={executor.robot_id}, "
                f"source={source_position}, "
                f"target={target_position}"
            )

        if not destination.pending_box_pick:
            raise ValueError(
                f"moveBox rejected for {destination.station_id}: "
                "no confirmed unconsumed box detection"
            )

        destination.pending_box_pick = False

        logging.info(
            "Executing moveBox requestId=%s stationId=%s robotId=%s "
            "source=%s target=%s",
            envelope.get("requestId"),
            destination.station_id,
            executor.robot_id,
            source_position,
            target_position,
        )


        # t4: when moveBox is invoked in this server.
        await destination._capture_t4_and_log_pair(
            envelope.get("requestId"),
            envelope.get("runId"),
        )

        async with executor.robot_lock:
            await executor._execute_robot_sequence(sequence)

        await destination.restart_conveyor_if_safe()

    
    async def _op_move_to_home(self, envelope: Dict[str, Any], params: Dict[str, Any]) -> None:
        executor = envelope.get("_executor") or self
        value = params.get("value", params.get("move"))
        move = self._coerce_bool(value)
        if move is None:
            raise ValueError(f"Invalid moveToHome payload: {params}")
        
        sequence = executor.robot_sequences.get("moveToHome", [])
        async with executor.robot_lock:
            await executor._execute_robot_sequence(sequence)

            logging.info("[%s] Robot sequence complete. Restarting conveyor.", executor.robot_id)
        await self.restart_conveyor_if_safe()
            

    async def _execute_robot_sequence(self, sequence: List[Dict[str, Any]]) -> None:
        await self.publish_robot_moving(True)
        try:
            for step in sequence:
                action = step.get("action")

                if action == "execute_command":
                    await self._execute_robot_command(int(step["cmd"]))
                    continue

                if action == "set_gripper":
                    await self.gripper_node.write_value(bool(step["value"]))
                    continue

                if action == "sleep":
                    await asyncio.sleep(float(step["seconds"]))
                    continue

                raise ValueError(f"Unsupported sequence action: {action}")
        finally:
            await self.exec_node.write_value(False)
            await self.publish_robot_moving(False)

    async def _execute_robot_command(self, command: int) -> None:
        # Done is a status output owned by the OIP robot. Wait until the robot
        # reports idle before issuing a new rising edge on Execute.
        await self._wait_for_robot_done(
            expected=True,
            timeout=self.ROBOT_READY_TIMEOUT_SECONDS,
            phase="ready",
            command=command,
        )

        # Hold Execute low long enough for OIP's polling loop to observe it.
        # Without this reset interval, a quick false -> true transition can be
        # missed and OIP will not see a new rising edge.
        await self.exec_node.write_value(False)
        await asyncio.sleep(self.EXECUTE_RESET_SECONDS)
        await self.cmd_node.write_value(ua.Variant(command, ua.VariantType.Int16))
        await self.exec_node.write_value(True)

        try:
            # OIP writes Done=False when it accepts the command and begins
            # moving. This prevents a stale Done=True value from being treated
            # as immediate command completion.
            await self._wait_for_robot_done(
                expected=False,
                timeout=self.ROBOT_START_TIMEOUT_SECONDS,
                phase="start",
                command=command,
            )
        finally:
            await self.exec_node.write_value(False)

        await self._wait_for_robot_done(
            expected=True,
            timeout=self.ROBOT_MOTION_TIMEOUT_SECONDS,
            phase="completion",
            command=command,
        )

    async def _wait_for_robot_done(
        self,
        expected: bool,
        timeout: float,
        phase: str,
        command: int,
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while True:
            done = bool(await self.done_node.get_value())
            if done == expected:
                logging.info(
                    "[%s] Robot command=%s phase=%s Done=%s",
                    self.station_id,
                    command,
                    phase,
                    done,
                )
                return

            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(
                    f"Robot command {command} timed out waiting for "
                    f"Done={expected} during {phase} after {timeout:.1f}s"
                )

            await asyncio.sleep(min(self.ROBOT_STATUS_POLL_SECONDS, remaining))



class ProductionLineController(StationOperationDispatcher):
    """
    Blueprint class to manage the independent state machine and 
    OPC UA data nodes for an individual production station.
    """
    def __init__(
        self,
        station_id,
        namespace_idx,
        idx_folder,
        mqtt_client,
        server_instance_id,
        robot_id=None,
        conveyor_id=None,
    ):
        self.station_id = station_id
        self.robot_id = robot_id or station_id.replace("Station_", "Robot_", 1)
        self.conveyor_id = conveyor_id or station_id.replace("Station_", "Conveyor_", 1)
        self.ns = namespace_idx
        self.folder = idx_folder
        self.mqtt = mqtt_client
        self.server_instance_id = server_instance_id
        self.robot_lock = asyncio.Lock()
        self.conveyor_lock = asyncio.Lock()
        self.operation_queue = asyncio.Queue()
        self.robot_ready = asyncio.Event()
  
        # State tracking flags persistent to THIS specific station instance
        self.waiting_for_pickup = False
        self.target_running = True
        self.target_speed = 1.0
        self.box_is_present = False
        self.detection_armed = True
        self.pending_box_pick = False
        self.present_sample_count = 0
        self.clear_started_at = None
        self.box_event_sequence = 0

        # State caches to enforce Report-by-Exception (no duplicate spam)
        self.last_running_state = None
        self.last_speed_state = None
        self.last_box_state = None
        self.last_executing_state = None
        self.last_fault_active = None
        
        # Node placeholders
        self.cmd_node = None
        self.exec_node = None
        self.done_node = None
        self.gripper_node = None
        self.fault_active_node = None
        self.conveyor_running = None
        self.conveyor_speed = None
        self.sensor_node = None

        self.logged_latency_samples = 0
        self.current_run_id = None
        self.pending_t0 = None
        self.log_lock = asyncio.Lock()
        self.log_csv_path = os.path.join(os.path.dirname(__file__), "OIP_server_logs.csv")
        self._ensure_log_file_header()

        # Cached dispatcher config
        self.operation_handlers = self._build_operation_handlers()
        self.robot_sequences = self._build_robot_sequences()

    def _ensure_log_file_header(self):
        expected_header = [
            "request_id",
            "run_id",
            "station_id",
            "sample_in_run",
            "t0_unix",
            "t4_unix",
            "status",
        ]

        needs_header = (not os.path.exists(self.log_csv_path)) or os.path.getsize(self.log_csv_path) == 0
        if needs_header:
            with open(self.log_csv_path, "a", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(expected_header)
            return

        with open(self.log_csv_path, "r", newline="", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            rows = list(reader)

        if not rows:
            with open(self.log_csv_path, "w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(expected_header)
            return

        current_header = rows[0]
        if current_header == expected_header:
            return

        raise RuntimeError(
            f"Unexpected CSV header in {self.log_csv_path}. "
            "Move or rename the previous log before starting a new run. "
            f"Expected {expected_header}, got {current_header}."
        )

    def _capture_t0_if_needed(self):
        self.pending_t0 = time.time()

    async def _capture_t4_and_log_pair(self, request_id, run_id):
        async with self.log_lock:
            if self.pending_t0 is None:
                logging.warning("[%s] No pending t0 available when capturing t4.", self.station_id)
                return
            if run_id != self.current_run_id:
                self.current_run_id = run_id
                self.logged_latency_samples = 0

            t0 = self.pending_t0
            self.pending_t0 = None
            t4 = time.time()
            sample_index = self.logged_latency_samples + 1

            with open(self.log_csv_path, "a", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([
                    request_id or "",
                    run_id or "",
                    self.station_id,
                    sample_index,
                    f"{t0:.6f}",
                    f"{t4:.6f}",
                    "started",
                ])

            self.logged_latency_samples = sample_index
            logging.info(
                "[%s] Logged requestId=%s runId=%s sample %d "
                "to %s (t0=%.6f, t4=%.6f)",
                self.station_id,
                request_id,
                run_id,
                self.logged_latency_samples,
                self.log_csv_path,
                t0,
                t4,
            )

    def _read_payload(self, payload_bytes):
        payload_text = payload_bytes.decode("utf-8").strip()
        if not payload_text:
            return None
        try:
            return json.loads(payload_text)
        except json.JSONDecodeError:
            return payload_text

    def _coerce_bool(self, value):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "on", "yes"}:
                return True
            if normalized in {"false", "0", "off", "no"}:
                return False
        if isinstance(value, (int, float)):
            return bool(value)
        return None

    def _coerce_float(self, value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    async def handle_operation_message(self, operation_name, payload_bytes, selected_executor=None):
        payload = self._read_payload(payload_bytes)
        await self.dispatch_operation(operation_name, payload, selected_executor)

    async def run_operation_worker(self):
        while True:
            queued_operation = await self.operation_queue.get()
            try:
                operation_name, payload_bytes, *executor = queued_operation
                await self.handle_operation_message(
                    operation_name,
                    payload_bytes,
                    executor[0] if executor else None,
                )
            except Exception:
                logging.exception("[%s] Queued operation failed to dispatch %s", self.station_id, operation_name)
            finally:
                self.operation_queue.task_done()

    async def initialize_nodes(self):
        """Creates unique OPC UA folders and variables for this specific station."""
        # Create a unique sub-folder for this station (e.g., Station_01)
        station_folder = await self.folder.add_object(self.ns, self.station_id)
        
        robot_object = await station_folder.add_object(self.ns, "Robot")
        conveyor_object = await station_folder.add_object(self.ns, "ConveyorBelt")


        # Robot Nodes
        self.cmd_node = await robot_object.add_variable(self.ns, "Command", 1, varianttype=ua.VariantType.Int16)
        self.exec_node = await robot_object.add_variable(self.ns, "Execute", False, varianttype=ua.VariantType.Boolean)
        self.done_node = await robot_object.add_variable(self.ns, "Done", False, varianttype=ua.VariantType.Boolean)
        self.gripper_node = await robot_object.add_variable(self.ns, "GripperState", False, varianttype=ua.VariantType.Boolean)
        self.fault_active_node = await robot_object.add_variable(self.ns, "FaultActive", False, varianttype=ua.VariantType.Boolean)

        # Conveyor Belt Nodes
        self.conveyor_running = await conveyor_object.add_variable(self.ns, "Running", True, varianttype=ua.VariantType.Boolean)
        self.conveyor_speed = await conveyor_object.add_variable(self.ns, "Speed", 1.0, varianttype=ua.VariantType.Float)
        self.sensor_node = await conveyor_object.add_variable(self.ns, "LaserSensor", 0.0, varianttype=ua.VariantType.Float)
        
        # Make all nodes writable by the simulation
        await self.cmd_node.set_writable()
        await self.exec_node.set_writable()
        await self.done_node.set_writable()
        await self.gripper_node.set_writable()
        await self.fault_active_node.set_writable()
        await self.conveyor_running.set_writable()
        await self.conveyor_speed.set_writable()
        await self.sensor_node.set_writable()
        print(f"[INFO] Initialized and mapped nodes for {self.station_id}")

    async def publish_initial_state(self):
        """Publish initial conveyor state once so external systems receive startup values."""
        running = await self.conveyor_running.get_value()
        speed = await self.conveyor_speed.get_value()
        fault_active = bool(await self.fault_active_node.get_value())

        await self.publish_conveyor_running(bool(running))
        await self.publish_conveyor_speed(float(speed))
        await self.publish_box_detected(False)
        await self.publish_robot_moving(False)
        await self.publish_fault_active(fault_active)


    async def publish_conveyor_running(self, running):
        if running != self.last_running_state:
            topic_running = f"factory/conveyors/{self.conveyor_id}/telemetry/isRunning"
            payload = json.dumps({
                "value": running,
                "stationId": self.station_id,
                "conveyorId": self.conveyor_id,
            })
            await self.mqtt.publish(topic_running, payload, qos=1, retain=True)
            self.last_running_state = running

    async def publish_box_detected(self, box_detected):
        if box_detected != self.last_box_state:
            topic_box = f"factory/conveyors/{self.conveyor_id}/telemetry/boxDetected"
            self.box_event_sequence += 1
            payload = {
                "value": box_detected,
                "boxDetected": box_detected,
                "stationId": self.station_id,
                "conveyorId": self.conveyor_id,
                "eventId": (f"{self.station_id}:{self.server_instance_id}:box:{self.box_event_sequence:06d}"),
            }         
            await self.mqtt.publish(topic_box, json.dumps(payload), qos=1, retain=True)
            self.last_box_state = box_detected

    async def publish_conveyor_speed(self, speed):
        if speed != self.last_speed_state:
            topic_speed = f"factory/conveyors/{self.conveyor_id}/telemetry/currentSpeed"
            payload = json.dumps({
                "value": speed,
                "stationId": self.station_id,
                "conveyorId": self.conveyor_id,
            })
            await self.mqtt.publish(topic_speed, payload, qos=1, retain=True)
            self.last_speed_state = speed
    
    async def publish_robot_moving(self, moving):
        if moving != self.last_executing_state:
            topic_moving = f"factory/robots/{self.robot_id}/telemetry/isMoving"
            payload = json.dumps({
                "value": moving,
                "stationId": self.station_id,
                "robotId": self.robot_id,
            })
            await self.mqtt.publish(topic_moving, payload, qos=1, retain=True)
            self.last_executing_state = moving
    
    async def publish_station_status(self) -> None:
        topic = f"simulation/{self.station_id}/status"
        payload = {
            "stationId": self.station_id,
            "online": True,
            "robotReady": self.robot_ready.is_set(),
            "serverInstanceId": self.server_instance_id,
            "timestamp": time.time(),
        }
        await self.mqtt.publish(topic, json.dumps(payload), qos=1, retain=True)

    async def publish_fault_active(self, fault_active: bool) -> None:
        if fault_active == self.last_fault_active:
            return

        timestamp_ns = time.time_ns()
        topic = f"factory/robots/{self.robot_id}/telemetry/faultActive"
        payload = {
            "value": fault_active,
            "stationId": self.station_id,
            "robotId": self.robot_id,
            "faultActive": fault_active,
            "eventId": f"{self.robot_id}-faultActive-{timestamp_ns}",
            "timestampNs": timestamp_ns,
        }

        await self.mqtt.publish(
            topic,
            json.dumps(payload),
            qos=1,
            retain=True,
        )

        self.last_fault_active = fault_active

    async def run_robot_readiness_monitor(self, mqtt_listener_ready: asyncio.Event) -> None:
        await mqtt_listener_ready.wait()
        await self.publish_station_status()

        while not self.robot_ready.is_set():
            done = bool(await self.done_node.get_value())
            execute = bool(await self.exec_node.get_value())
            if done and not execute:
                self.robot_ready.set()
                await self.publish_station_status()
                return

            await asyncio.sleep(self.ROBOT_STATUS_POLL_SECONDS)

    async def run_fault_monitor(self) -> None:
        """Forward changes from the OPC UA fault node to MQTT/AAS."""
        while True:
            try:
                fault_active = bool(await self.fault_active_node.get_value())
                await self.publish_fault_active(fault_active)
            except Exception:
                logging.exception(
                    "[%s] Failed to monitor robot fault state",
                    self.station_id,
                )

            await asyncio.sleep(self.ROBOT_STATUS_POLL_SECONDS)

    async def publish_operation_status(
        self,
        operation: str,
        request_id: Any,
        status: str,
        error: str | None = None,
        robot_id: str | None = None,
    ) -> None:

        """Publish a correlated acknowledgement for an operation command."""
        logging.info(
            "Operation status requestId=%s stationId=%s robotId=%s operation=%s status=%s",
            request_id,
            self.station_id,
            robot_id,
            operation,
            status,
        )
        if not request_id:
            return
        topic = f"simulation/{self.station_id}/replies/{operation}"
        payload = {
            "requestId": request_id,
            "stationId": self.station_id,
            "operation": operation,
            "status": status,
            "timestamp": time.time(),
        }
        if robot_id:
            payload["robotId"] = robot_id
        if error:
            payload["error"] = error
        json_payload = json.dumps(payload)
        await self.mqtt.publish(topic, json_payload, qos=1, retain=False)

    async def stop_conveyor_for_box(self):
            async with self.conveyor_lock:
                await self.conveyor_running.write_value(False)
                await self.conveyor_speed.write_value(ua.Variant(0.0, ua.VariantType.Float))
                await self.publish_conveyor_running(False)
                await self.publish_conveyor_speed(0.0)

    async def restart_conveyor_if_safe(self) -> bool:
        async with self.conveyor_lock:
            if self.box_is_present:
                logging.info("[%s] Cannot restart conveyor: box still present.", self.station_id)
                return False
            if not self.target_running:
                return False
            await self.conveyor_running.write_value(True)
            await self.conveyor_speed.write_value(ua.Variant(self.target_speed, ua.VariantType.Float))
            await self.publish_conveyor_running(True)
            await self.publish_conveyor_speed(self.target_speed)
            return True

    async def run_cyclical_logic(self):
        """Your exact pick-and-place logic sequence, running independently for this line."""
        print(f"[DIAGNOSTIC] Monitoring Laser Sensor for {self.station_id}...")
        
        while True:
            await asyncio.sleep(0.05)

            current_distance = float(await self.sensor_node.get_value())

            raw_box_present = 0.01 < current_distance < 0.5
            now = asyncio.get_running_loop().time()

            if raw_box_present:
                self.clear_started_at = None
                self.present_sample_count += 1

                presence_confirmed = (self.present_sample_count >= self.BOX_PRESENT_CONFIRM_SAMPLES)

                if presence_confirmed and self.detection_armed:
                    # Disarm until the sensor has been stably clear.
                    self.detection_armed = False
                    self.pending_box_pick = True
                    self.box_is_present = True
                    self.waiting_for_pickup = True

                    self._capture_t0_if_needed()

                    logging.info(
                        "[%s] Confirmed box at distance %.3f; pick token created.",
                        self.station_id,
                        current_distance,
                    )

                    await self.stop_conveyor_for_box()
                    await self.publish_box_detected(True)

            else:
                self.present_sample_count = 0

                if self.clear_started_at is None:
                    self.clear_started_at = now

                clear_duration = now - self.clear_started_at
                clear_confirmed = (clear_duration >= self.BOX_CLEAR_DEBOUNCE_SECONDS)

                if clear_confirmed:
                    if self.box_is_present:
                        self.box_is_present = False
                        self.waiting_for_pickup = False

                        logging.info(
                            "[%s] Sensor stably clear for %.3f seconds.",
                            self.station_id,
                            clear_duration,
                        )

                        await self.publish_box_detected(False)
                        await self.restart_conveyor_if_safe()

                    # This does not depend on robot_lock. A new box may therefore
                    # be detected while the previous robot operation is running.
                    if not self.detection_armed:
                        self.detection_armed = True
                        logging.info(
                            "[%s] Box detection re-armed.",
                            self.station_id,
                        )

            is_currently_busy = self.robot_lock.locked()
            await self.publish_robot_moving(is_currently_busy)
            

async def mqtt_operation_listener(mqtt_client, controllers_by_station, listener_ready=None, controllers_by_robot=None):
    operation_topics = (
        "simulation/+/operations/+",
        "simulation/robots/+/operations/+",
    )
    for operation_topic in operation_topics:
        await mqtt_client.subscribe(operation_topic)
        logging.info("MQTT operation listener subscribed to %s", operation_topic)
    if listener_ready is not None:
        listener_ready.set()

    if controllers_by_robot is None:
        controllers_by_robot = {
            controller.robot_id: controller
            for controller in controllers_by_station.values()
        }
    controllers_by_station_ci = {
        station_id.lower(): controller
        for station_id, controller in controllers_by_station.items()
    }
    controllers_by_robot_ci = {
        robot_id.lower(): controller
        for robot_id, controller in controllers_by_robot.items()
    }

    def _resolve_target(topic_parts):
        if len(topic_parts) == 4 and topic_parts[0] == "simulation" and topic_parts[2] == "operations":
            return "station", topic_parts[1], topic_parts[3]
        if (
            len(topic_parts) == 5
            and topic_parts[0] == "simulation"
            and topic_parts[1] == "robots"
            and topic_parts[3] == "operations"
        ):
            return "robot", topic_parts[2], topic_parts[4]
        return None, None, None

    def _payload_envelope(payload_bytes):
        try:
            if isinstance(payload_bytes, bytes):
                payload_bytes = payload_bytes.decode("utf-8")
            payload = json.loads(payload_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    async def _reject_robot_request(destination, operation, envelope, robot_id, error):
        request_id = envelope.get("requestId")
        station_id = envelope.get("stationId")
        logging.warning(
            "Robot operation rejected requestId=%s stationId=%s robotId=%s "
            "operation=%s status=failed error=%s",
            request_id,
            station_id,
            robot_id,
            operation,
            error,
        )
        if destination is not None:
            await destination.publish_operation_status(
                operation,
                request_id,
                "failed",
                error,
                robot_id=robot_id,
            )

    async def process_messages(messages):
        pending_tasks = set()

        def _on_task_done(task: asyncio.Task) -> None:
            pending_tasks.discard(task)
            try:
                task.result()
            except Exception:
                logging.exception("Station operation task failed")

        async for message in messages:
            logging.info("MQTT RX topic=%s payload=%s", message.topic, message.payload)
            topic_parts = str(message.topic).split("/")
            route_type, route_id, operation_name = _resolve_target(topic_parts)
            if route_type is None or route_id is None or operation_name is None:
                logging.warning("Ignoring malformed topic: %s", message.topic)
                continue

            if route_type == "station":
                controller = controllers_by_station.get(route_id)
                if controller is None and isinstance(route_id, str):
                    controller = controllers_by_station_ci.get(route_id.lower())
                if controller is None:
                    logging.warning("MQTT operation received for unknown station: %s", route_id)
                    continue
                await controller.operation_queue.put((operation_name, message.payload, controller))
                continue

            envelope = _payload_envelope(message.payload)
            station_id = envelope.get("stationId")
            if not isinstance(station_id, str) or not station_id.strip():
                await _reject_robot_request(
                    None,
                    operation_name,
                    envelope,
                    route_id,
                    "stationId is required for robot-routed operations",
                )
                continue
            station_id = station_id.strip()
            destination = controllers_by_station.get(station_id)
            if destination is None:
                destination = controllers_by_station_ci.get(station_id.lower())
            if destination is None:
                await _reject_robot_request(
                    None,
                    operation_name,
                    envelope,
                    route_id,
                    f"Unknown destination station '{station_id}'",
                )
                continue

            if operation_name.strip().lower() not in StationOperationDispatcher.ROBOT_OPERATIONS:
                await _reject_robot_request(
                    destination,
                    operation_name,
                    envelope,
                    route_id,
                    "Robot-specific topics only accept robot operations",
                )
                continue

            payload_robot_id = envelope.get("robotId")
            if payload_robot_id is not None and (
                not isinstance(payload_robot_id, str)
                or payload_robot_id.lower() != route_id.lower()
            ):
                await _reject_robot_request(
                    destination,
                    operation_name,
                    envelope,
                    route_id,
                    f"Topic robotId '{route_id}' does not match payload robotId '{payload_robot_id}'",
                )
                continue

            executor = controllers_by_robot.get(route_id)
            if executor is None:
                executor = controllers_by_robot_ci.get(route_id.lower())
            if executor is None:
                await _reject_robot_request(
                    destination,
                    operation_name,
                    envelope,
                    route_id,
                    f"Unknown robot '{route_id}'",
                )
                continue

            await destination.operation_queue.put((operation_name, message.payload, executor))
            
    messages_source = mqtt_client.messages
    if callable(messages_source):
        messages_source = messages_source()

    if hasattr(messages_source, "__aenter__"):
        async with messages_source as messages:
            await process_messages(messages)
    else:
        await process_messages(messages_source)

async def publish_station_manifests(mqtt_client: MqttClient) -> None:
    """Publish retained station manifests for automatic gateway discovery."""
    manifest_path = os.getenv(
        "STATION_MANIFESTS_FILE",
        os.path.join("basyx-setup", "mqtt-aas-bridge", "manifests.json"),
    )
    if not os.path.exists(manifest_path):
        logging.warning("Station manifest file not found: %s", manifest_path)
        return

    with open(manifest_path, "r", encoding="utf-8") as manifest_file:
        manifests = json.load(manifest_file)
    if not isinstance(manifests, dict):
        raise ValueError("Station manifest file must contain an object keyed by station ID")

    for station_id, manifest in manifests.items():
        if not isinstance(manifest, dict):
            continue
        normalized_station = str(station_id).strip().lower()
        topic = f"factory/{normalized_station}/manifest"
        await mqtt_client.publish(topic, json.dumps(manifest), qos=1, retain=True)
        logging.info("Published retained station manifest to %s", topic)

async def main():
    server = Server()
    # Keep OIP client sessions alive across long simulation runs.
    session_timeout_ms = float(os.getenv("OPCUA_SESSION_TIMEOUT_MS", "86400000"))
    server.iserver.min_session_timeout_ms = session_timeout_ms
    server.iserver.max_session_timeout_ms = session_timeout_ms
    await server.init()
    server.iserver.callback_service.addListener(
        CallbackType.PostWrite,
        log_failed_opcua_writes,
    )
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    endpoint = "opc.tcp://0.0.0.0:4840"
    server.set_endpoint(endpoint)
    server.set_server_name("Simulation Server")

    uri = "http://openindustryproject.github.io/robot0"
    idx = await server.register_namespace(uri)

    objects_folder = server.nodes.objects
    factory_object = await objects_folder.add_object(idx, "FactoryFloor")

    station_ids = [
        station_id.strip()
        for station_id in os.getenv("STATION_IDS", "Station_01,Station_02").split(",")
        if station_id.strip()
    ]
    controllers_by_station = {}
    controllers_by_robot = {}
    server_instance_id = str(uuid.uuid4())
    server_status_topic = "simulation/server/status"
    server_offline_payload = json.dumps({
        "online": False,
        "serverInstanceId": server_instance_id,
        "timestamp": time.time(),
    })
    server_will = Will(
        topic=server_status_topic,
        payload=server_offline_payload,
        qos=1,
        retain=True,
    )

    try:
        async with MqttClient("localhost", will=server_will) as mqtt_client, server:
            await mqtt_client.publish(
                server_status_topic,
                json.dumps({
                    "online": True,
                    "serverInstanceId": server_instance_id,
                    "timestamp": time.time(),
                }),
                qos=1,
                retain=True,
            )
            await publish_station_manifests(mqtt_client)
            for s_id in station_ids:
                robot_id = s_id.replace("Station_", "Robot_", 1)
                controller = ProductionLineController(
                    s_id,
                    idx,
                    factory_object,
                    mqtt_client,
                server_instance_id,
                robot_id=robot_id,
                conveyor_id=s_id.replace("Station_", "Conveyor_", 1),
            )
                await controller.initialize_nodes()
                await controller.publish_initial_state()
                controllers_by_station[s_id] = controller
                controllers_by_robot[robot_id] = controller

            print(f"\n[INFO] Unified OPC UA + MQTT Gateway Environment Online!")
            mqtt_listener_ready = asyncio.Event()
            tasks = [
                mqtt_operation_listener(
                    mqtt_client,
                    controllers_by_station,
                    listener_ready=mqtt_listener_ready,
                    controllers_by_robot=controllers_by_robot,
                )
            ]

            for controller in controllers_by_station.values():
                tasks.append(controller.run_cyclical_logic())
                tasks.append(controller.run_operation_worker())
                tasks.append(controller.run_robot_readiness_monitor(mqtt_listener_ready))
                tasks.append(controller.run_fault_monitor())

            try:
                await asyncio.gather(*tasks)
            finally:
                await mqtt_client.publish(
                    server_status_topic,
                    json.dumps({
                        "online": False,
                        "serverInstanceId": server_instance_id,
                        "timestamp": time.time(),
                    }),
                    qos=1,
                    retain=True,
                )
    except MqttError as exc:
        logging.error(
            "MQTT connection failed (%s). Ensure a broker is running at localhost:1883.",
            exc,
        )
        raise

if __name__ == "__main__":
    try:
        # aiomqtt/paho uses add_reader/add_writer, which requires Selector loop on Windows.
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Server stopped by user.")
