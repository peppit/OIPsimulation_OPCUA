import asyncio
import logging
import json
import sys
from asyncua import Server, ua
from aiomqtt import Client as MqttClient, MqttError

logging.basicConfig(level=logging.INFO)
logging.getLogger("asyncua.server.address_space").setLevel(logging.WARNING)
logging.getLogger("asyncua.server.standard_address_space").setLevel(logging.WARNING)



class ProductionLineController:
    """
    Blueprint class to manage the independent state machine and 
    OPC UA data nodes for an individual production station.
    """
    def __init__(self, station_id, namespace_idx, idx_folder, mqtt_client):
        self.station_id = station_id
        self.ns = namespace_idx
        self.folder = idx_folder
        self.mqtt = mqtt_client
        
        # State tracking flags persistent to THIS specific station instance
        self.waiting_for_pickup = False
        self.target_running = True
        self.target_speed = 1.0

        # State caches to enforce Report-by-Exception (no duplicate spam)
        self.last_running_state = None
        self.last_speed_state = None
        self.last_box_state = None
        
        # Node placeholders
        self.cmd_node = None
        self.exec_node = None
        self.done_node = None
        self.gripper_node = None
        self.conveyor_running = None
        self.conveyor_speed = None
        self.sensor_node = None

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

        if operation_name == "conveyorRunning":
            if isinstance(payload, dict):
                value = payload.get("value", payload.get("running"))
            else:
                value = payload
            running = self._coerce_bool(value)
            if running is None:
                logging.warning("[%s] Invalid conveyorRunning payload: %s", self.station_id, payload)
                return
            self.target_running = running
            await self.conveyor_running.write_value(running)
            await self.publish_conveyor_running(running)
            logging.info("[%s] Applied operation conveyorRunning=%s", self.station_id, running)

        elif operation_name == "conveyorSpeed":
            if isinstance(payload, dict):
                value = payload.get("value", payload.get("speed"))
            else:
                value = payload
            speed = self._coerce_float(value)
            if speed is None:
                logging.warning("[%s] Invalid conveyorSpeed payload: %s", self.station_id, payload)
                return
            if speed < 0.0:
                logging.warning("[%s] Ignoring negative conveyorSpeed: %s", self.station_id, speed)
                return
            self.target_speed = speed
            await self.conveyor_speed.write_value(ua.Variant(float(speed), ua.VariantType.Float))
            await self.publish_conveyor_speed(float(speed))
            logging.info("[%s] Applied operation conveyorSpeed=%s", self.station_id, speed)

        else:
            logging.warning("[%s] Unknown operation '%s' with payload: %s", self.station_id, operation_name, payload)

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

    async def run_cyclical_logic(self):
        """Your exact pick-and-place logic sequence, running independently for this line."""
        print(f"[DIAGNOSTIC] Monitoring Laser Sensor for {self.station_id}...")
        
        while True:
            await asyncio.sleep(0.05)

            # Read raw distance value from OIP for this station
            current_distance = await self.sensor_node.get_value()
            # Apply your exact trigger calculation rule
            box_is_present = (current_distance > 0.01) and (current_distance < 0.5)
            await self.publish_box_detected(box_is_present, current_distance)

            if box_is_present:
                logging.info("[%s] Box detected, stopping conveyor", self.station_id)
                await self.conveyor_running.write_value(False)
                await self.conveyor_speed.write_value(ua.Variant(0.0, ua.VariantType.Float))
                await self.publish_conveyor_running(False)
                await self.publish_conveyor_speed(0.0)

            


async def mqtt_operation_listener(mqtt_client, controllers_by_station):
    operation_topic = "simulation/+/operations/+"
    await mqtt_client.subscribe(operation_topic)
    logging.info("MQTT operation listener subscribed to %s", operation_topic)

    async def process_messages(messages):
        async for message in messages:
            logging.info("MQTT RX topic=%s payload=%s", message.topic, message.payload)
            topic_parts = str(message.topic).split("/")
            if len(topic_parts) != 4:
                logging.warning("Ignoring malformed topic: %s", message.topic)
                continue
            _, station_id, _, operation_name = topic_parts
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