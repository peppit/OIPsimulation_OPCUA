import asyncio
import logging
import json
import sys
from typing import Any, Awaitable, Callable, Dict, List
from asyncua import Server, ua
from aiomqtt import Client as MqttClient, MqttError

logging.basicConfig(level=logging.INFO)
logging.getLogger("asyncua.server.address_space").setLevel(logging.WARNING)
logging.getLogger("asyncua.server.standard_address_space").setLevel(logging.WARNING)


OperationHandler = Callable[[Dict[str, Any], Dict[str, Any]], Awaitable[None]]


class StationOperationDispatcher:
    """
    Config-driven operation dispatcher for station controllers.

    This class is designed to be mixed into the station controller class.
    Required members on self:
    - station_id
    - cmd_node, exec_node, gripper_node, conveyor_running, conveyor_speed
    - target_running, target_speed
    - publish_conveyor_running(), publish_conveyor_speed(), publish_robot_moving()
    - _coerce_bool(), _coerce_float()
    - operation_lock
    """

    def _build_operation_handlers(self) -> Dict[str, OperationHandler]:
        return {
            "conveyorrunning": self._op_conveyor_running,
            "conveyorspeed": self._op_conveyor_speed,
            "movebox": self._op_move_box,
        }

    def _build_operation_aliases(self) -> Dict[str, str]:
        return {
            "running": "conveyorRunning",
            "speed": "conveyorSpeed",
            "move_box": "moveBox",
            "movebox": "moveBox",
            "moveBox": "moveBox",
        }

    def _build_robot_sequences(self) -> Dict[str, List[Dict[str, Any]]]:
        # Keep station-agnostic defaults here. Override per station if needed.
        return {
            "moveBox": [
                {"action": "set_done", "value": False},
                {"action": "set_cmd_exec", "cmd": 2, "exec": True},
                {"action": "sleep", "seconds": 2.0},
                {"action": "set_exec", "value": False},
                {"action": "set_gripper", "value": True},
                {"action": "sleep", "seconds": 1.0},
                {"action": "set_cmd_exec", "cmd": 3, "exec": True},
                {"action": "sleep", "seconds": 1.5},
                {"action": "set_exec", "value": False},
                {"action": "sleep", "seconds": 0.5},
                {"action": "set_cmd_exec", "cmd": 4, "exec": True},
                {"action": "sleep", "seconds": 2.0},
                {"action": "set_exec", "value": False},
                {"action": "sleep", "seconds": 0.5},
                {"action": "set_cmd_exec", "cmd": 5, "exec": True},
                {"action": "sleep", "seconds": 2.0},
                {"action": "set_exec", "value": False},
                {"action": "set_gripper", "value": False},
                {"action": "sleep", "seconds": 1.5},
                {"action": "set_exec", "value": False},
                {"action": "sleep", "seconds": 0.5},
                {"action": "set_cmd_exec", "cmd": 4, "exec": True},
                {"action": "sleep", "seconds": 1.0},
                {"action": "set_exec", "value": False},
                {"action": "set_done", "value": True},
            ]
        }

    async def dispatch_operation(self, operation_name: str, payload: Any) -> None:
        operation_name, payload_envelope = self._normalize_operation_message(operation_name, payload)

        station_from_payload = payload_envelope.get("stationId")
        if station_from_payload and station_from_payload != self.station_id:
            logging.warning(
                "[%s] Ignoring operation for different station '%s': %s",
                self.station_id,
                station_from_payload,
                payload_envelope,
            )
            return

        key = operation_name.strip().lower()
        handler = self.operation_handlers.get(key)

        if handler is None:
            logging.warning(
                "[%s] Unknown operation '%s' with payload: %s",
                self.station_id,
                operation_name,
                payload_envelope,
            )
            return

        try:
            await handler(payload_envelope, payload_envelope.get("params", {}))
        except Exception:
            logging.exception("[%s] Operation '%s' failed", self.station_id, operation_name)

    def _normalize_operation_message(self, operation_name: str, payload: Any) -> Any:
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
                canonical_op = self._canonical_operation_name(op_from_payload)
                envelope = {
                    "requestId": payload.get("requestId"),
                    "stationId": payload.get("stationId", self.station_id),
                    "operation": canonical_op,
                    "params": params,
                    "raw": payload,
                }
                return canonical_op, envelope

            # Legacy single-operation payloads.
            canonical_op = self._canonical_operation_name(operation_name)
            envelope = {
                "requestId": payload.get("requestId"),
                "stationId": self.station_id,
                "operation": canonical_op,
                "params": payload,
                "raw": payload,
            }
            return canonical_op, envelope

        canonical_op = self._canonical_operation_name(operation_name)
        return canonical_op, {
            "requestId": None,
            "stationId": self.station_id,
            "operation": canonical_op,
            "params": {"value": payload},
            "raw": payload,
        }

    def _canonical_operation_name(self, name: str) -> str:
        aliases = self.operation_aliases
        return aliases.get(name, aliases.get(name.lower(), name))

    async def _op_conveyor_running(self, envelope: Dict[str, Any], params: Dict[str, Any]) -> None:
        value = params.get("value", params.get("running"))
        running = self._coerce_bool(value)
        if running is None:
            raise ValueError(f"Invalid conveyorRunning payload: {params}")

        async with self.operation_lock:
            self.target_running = running
            await self.conveyor_running.write_value(running)
            await self.publish_conveyor_running(running)
        logging.info("[%s] Applied operation conveyorRunning=%s", self.station_id, running)

    async def _op_conveyor_speed(self, envelope: Dict[str, Any], params: Dict[str, Any]) -> None:
        value = params.get("value", params.get("speed"))
        speed = self._coerce_float(value)
        if speed is None:
            raise ValueError(f"Invalid conveyorSpeed payload: {params}")
        if speed < 0.0:
            raise ValueError(f"Negative conveyorSpeed is invalid: {speed}")

        async with self.operation_lock:
            self.target_speed = speed
            await self.conveyor_speed.write_value(ua.Variant(float(speed), ua.VariantType.Float))
            await self.publish_conveyor_speed(float(speed))
        logging.info("[%s] Applied operation conveyorSpeed=%s", self.station_id, speed)

    async def _op_move_box(self, envelope: Dict[str, Any], params: Dict[str, Any]) -> None:
        conveyor = params.get("Conveyor1")
        pallet = params.get("Pallet1")
        if not conveyor or not pallet:
            raise ValueError(f"moveBox requires Conveyor1 and Pallet1, got: {params}")

        logging.info(
            "[%s] Executing moveBox conveyor=%s pallet=%s requestId=%s",
            self.station_id,
            conveyor,
            pallet,
            envelope.get("requestId"),
        )

        sequence = self.robot_sequences.get("moveBox", [])
        async with self.operation_lock:
            await self._execute_robot_sequence(sequence)

            # 2. Sequence complete! Automatically restart the conveyor to bring the next box
            logging.info("[%s] Robot sequence complete. Restarting conveyor.", self.station_id)
            
            # Use your saved target states to bring it back to its original configured speed
            await self.conveyor_running.write_value(self.target_running)
            await self.conveyor_speed.write_value(ua.Variant(self.target_speed, ua.VariantType.Float))
            
            # Broadcast the updated status changes out to the MQTT network
            await self.publish_conveyor_running(self.target_running)
            await self.publish_conveyor_speed(self.target_speed)
        

    async def _execute_robot_sequence(self, sequence: List[Dict[str, Any]]) -> None:
        await self.publish_robot_moving(True)
        try:
            for step in sequence:
                action = step.get("action")

                if action == "set_cmd_exec":
                    cmd = int(step["cmd"])
                    exec_value = bool(step.get("exec", True))
                    await self.cmd_node.write_value(ua.Variant(cmd, ua.VariantType.Int16))
                    await self.exec_node.write_value(exec_value)
                    continue

                if action == "set_exec":
                    await self.exec_node.write_value(bool(step["value"]))
                    continue

                if action == "set_gripper":
                    await self.gripper_node.write_value(bool(step["value"]))
                    continue

                if action == "set_done":
                    await self.done_node.write_value(bool(step["value"]))
                    continue

                if action == "sleep":
                    await asyncio.sleep(float(step["seconds"]))
                    continue

                raise ValueError(f"Unsupported sequence action: {action}")
        finally:
            await self.exec_node.write_value(False)
            await self.publish_robot_moving(False)



class ProductionLineController(StationOperationDispatcher):
    """
    Blueprint class to manage the independent state machine and 
    OPC UA data nodes for an individual production station.
    """
    def __init__(self, station_id, namespace_idx, idx_folder, mqtt_client):
        self.station_id = station_id
        self.ns = namespace_idx
        self.folder = idx_folder
        self.mqtt = mqtt_client
        self.operation_lock = asyncio.Lock()
        
        # State tracking flags persistent to THIS specific station instance
        self.waiting_for_pickup = False
        self.target_running = True
        self.target_speed = 1.0

        # State caches to enforce Report-by-Exception (no duplicate spam)
        self.last_running_state = None
        self.last_speed_state = None
        self.last_box_state = None
        self.last_executing_state = None
        
        # Node placeholders
        self.cmd_node = None
        self.exec_node = None
        self.done_node = None
        self.gripper_node = None
        self.conveyor_running = None
        self.conveyor_speed = None
        self.sensor_node = None

        # Cached dispatcher config
        self.operation_handlers = self._build_operation_handlers()
        self.operation_aliases = self._build_operation_aliases()
        self.robot_sequences = self._build_robot_sequences()

    async def _read_payload(self, payload_bytes):
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

    async def handle_operation_message(self, operation_name, payload_bytes):
        payload = await self._read_payload(payload_bytes)
        await self.dispatch_operation(operation_name, payload)

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

        # Conveyor Belt Nodes
        self.conveyor_running = await conveyor_object.add_variable(self.ns, "Running", False, varianttype=ua.VariantType.Boolean)
        self.conveyor_speed = await conveyor_object.add_variable(self.ns, "Speed", 0.0, varianttype=ua.VariantType.Float)
        self.sensor_node = await conveyor_object.add_variable(self.ns, "LaserSensor", 0.0, varianttype=ua.VariantType.Float)
        self.position_x_node = await conveyor_object.add_variable(self.ns, "PositionX", 0.0, varianttype=ua.VariantType.Float)
        self.position_y_node = await conveyor_object.add_variable(self.ns, "PositionY", 0.0, varianttype=ua.VariantType.Float)
        self.position_z_node = await conveyor_object.add_variable(self.ns, "PositionZ", 0.0, varianttype=ua.VariantType.Float)

        # Make all nodes writable by the simulation
        await self.cmd_node.set_writable()
        await self.exec_node.set_writable()
        await self.done_node.set_writable()
        await self.gripper_node.set_writable()
        await self.conveyor_running.set_writable()
        await self.conveyor_speed.set_writable()
        await self.sensor_node.set_writable()
        await self.position_x_node.set_writable()
        await self.position_y_node.set_writable()
        await self.position_z_node.set_writable()
        print(f"[INFO] Initialized and mapped nodes for {self.station_id}")


    async def publish_conveyor_running(self, running):
        if running != self.last_running_state:
            topic_running = f"simulation/{self.station_id}/isRunning"
            await self.mqtt.publish(topic_running, json.dumps({"isRunning": running}))
            print("PUB", topic_running, running)
            self.last_running_state = running

    async def publish_box_detected(self, box_detected, distance):
        if box_detected != self.last_box_state:
            topic_box = f"simulation/{self.station_id}/boxDetected"
            payload = {
                "boxDetected": box_detected,
                "distance": float(distance),
            }
            await self.mqtt.publish(topic_box, json.dumps(payload))
            print("PUB", topic_box, payload)
            self.last_box_state = box_detected

    async def publish_conveyor_speed(self, speed):
        if speed != self.last_speed_state:
            topic_speed = f"simulation/{self.station_id}/currentSpeed"
            await self.mqtt.publish(topic_speed, json.dumps({"currentSpeed": speed}))
            print("PUB", topic_speed, speed)
            self.last_speed_state = speed
    
    async def publish_robot_moving(self, moving):
        if moving != self.last_executing_state:
            topic_moving = f"simulation/{self.station_id}/isMoving"
            await self.mqtt.publish(topic_moving, json.dumps({"isMoving": moving}))
            print("PUB", topic_moving, moving)
            self.last_executing_state = moving

    async def run_cyclical_logic(self):
        """Your exact pick-and-place logic sequence, running independently for this line."""
        print(f"[DIAGNOSTIC] Monitoring Laser Sensor for {self.station_id}...")
        
        while True:
            await asyncio.sleep(0.05)

            current_distance = await self.sensor_node.get_value()
            box_is_present = (current_distance > 0.01) and (current_distance < 0.5)
            await self.publish_box_detected(box_is_present, current_distance)

            # Safety Auto-Stop: If a box arrives and the conveyor is running, stop it.
            if box_is_present and not self.operation_lock.locked():
                current_running = await self.conveyor_running.get_value()
                if current_running:
                    logging.info("[%s] Box detected automatically! Halting conveyor for pickup.", self.station_id)
                    await self.conveyor_running.write_value(False)
                    await self.conveyor_speed.write_value(ua.Variant(0.0, ua.VariantType.Float))
                    await self.publish_conveyor_running(False)
                    await self.publish_conveyor_speed(0.0)
            
            if not box_is_present and not self.operation_lock.locked():
                await self.publish_robot_moving(False)

            

async def mqtt_operation_listener(mqtt_client, controllers_by_station):
    operation_topics = [
        "simulation/+/operations/+",
        "simulation/robot/+",
    ]
    for operation_topic in operation_topics:
        await mqtt_client.subscribe(operation_topic)
        logging.info("MQTT operation listener subscribed to %s", operation_topic)

    def _try_extract_station_id_from_payload(payload_bytes):
        try:
            payload_text = payload_bytes.decode("utf-8").strip()
            if not payload_text:
                return None
            payload = json.loads(payload_text)
            if isinstance(payload, dict):
                station_id = payload.get("stationId")
                if isinstance(station_id, str) and station_id:
                    return station_id
        except Exception:
            return None
        return None

    def _resolve_target(topic_parts, payload_bytes):
        # Supported topic shapes:
        # 1) simulation/{stationId}/operations/{operation}
        # 2) simulation/robot/{operation} with stationId in payload
        if len(topic_parts) == 4 and topic_parts[0] == "simulation" and topic_parts[2] == "operations":
            return topic_parts[1], topic_parts[3]

        if len(topic_parts) == 3 and topic_parts[0] == "simulation" and topic_parts[1] == "robot":
            station_id = _try_extract_station_id_from_payload(payload_bytes)
            if station_id is None and len(controllers_by_station) == 1:
                station_id = next(iter(controllers_by_station.keys()))
            return station_id, topic_parts[2]

        return None, None

    async def process_messages(messages):
        async for message in messages:
            logging.info("MQTT RX topic=%s payload=%s", message.topic, message.payload)
            topic_parts = str(message.topic).split("/")
            station_id, operation_name = _resolve_target(topic_parts, message.payload)
            if station_id is None or operation_name is None:
                logging.warning("Ignoring malformed topic: %s", message.topic)
                continue
            controller = controllers_by_station.get(station_id)
            if controller is None:
                logging.warning("MQTT operation received for unknown station: %s", station_id)
                continue
            await controller.handle_operation_message(operation_name, message.payload)

    messages_source = mqtt_client.messages
    if callable(messages_source):
        messages_source = messages_source()

    if hasattr(messages_source, "__aenter__"):
        async with messages_source as messages:
            await process_messages(messages)
    else:
        await process_messages(messages_source)

async def main():
    server = Server()
    await server.init()
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    endpoint = "opc.tcp://0.0.0.0:4840"
    server.set_endpoint(endpoint)
    server.set_server_name("Simulation Server")

    uri = "http://openindustryproject.github.io/robot0"
    idx = await server.register_namespace(uri)

    objects_folder = server.nodes.objects
    factory_object = await objects_folder.add_object(idx, "FactoryFloor")

    station_ids = ["Station_01"]
    controllers = []
    controllers_by_station = {}

    try:
        async with MqttClient("localhost") as mqtt_client, server:
            for s_id in station_ids:
                controller = ProductionLineController(s_id, idx, factory_object, mqtt_client)
                await controller.initialize_nodes()
                controllers.append(controller)
                controllers_by_station[s_id] = controller

            print(f"\n[INFO] Unified OPC UA + MQTT Gateway Environment Online!")
            tasks = [mqtt_operation_listener(mqtt_client, controllers_by_station)]
            tasks.extend(controller.run_cyclical_logic() for controller in controllers)
            print(f"[INFO] All production line controllers are running. Press Ctrl+C to stop the server.")
            await asyncio.gather(*tasks)
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