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

        # State caches to enforce Report-by-Exception (no duplicate spam)
        self.last_running_state = None
        self.last_speed_state = None
        
        # Node placeholders
        self.cmd_node = None
        self.exec_node = None
        self.done_node = None
        self.gripper_node = None
        self.conveyor_running = None
        self.conveyor_speed = None
        self.sensor_node = None

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


    async def publish_mqtt(self, running, speed):

        changed_running = running != self.last_running_state
        changed_speed = speed != self.last_speed_state

        if changed_running:
            topic_running = f"simulation/{self.station_id}/isRunning"
            await self.mqtt.publish(topic_running, json.dumps({"isRunning": running}))
            print("PUB", topic_running, running)
            self.last_running_state = running

        if changed_speed:
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
            current_speed = await self.conveyor_speed.get_value()
            is_running = await self.conveyor_running.get_value()
            # Apply your exact trigger calculation rule
            box_is_present = (current_distance > 0.01) and (current_distance < 0.5)
            if box_is_present:
                print(f"[{self.station_id}] Box detected! Stopping conveyor...")
                await self.conveyor_running.write_value(False)
                await self.conveyor_speed.write_value(ua.Variant(0.0, ua.VariantType.Float))
                await self.publish_mqtt(False, 0.0)
                self.waiting_for_pickup = True
                await asyncio.sleep(0.5) # Friction stop buffer

            if box_is_present and self.waiting_for_pickup:
                
                # 1. Command: Move to Pick Position (Command 2)
                print(f"[{self.station_id}] Moving to pick position (Cmd 2)...")
                await self.cmd_node.write_value(ua.Variant(2, ua.VariantType.Int16))
                await self.exec_node.write_value(True)
                await asyncio.sleep(2.0)

                # 2. Actuate Gripper
                print(f"[{self.station_id}] Arrived! Actuating Gripper...")
                await self.exec_node.write_value(False) 
                await self.gripper_node.write_value(True)
                await asyncio.sleep(1.0)
                
                # 3. Command: Move to Place Position (Command 3)
                print(f"[{self.station_id}] Moving to place position (Cmd 3)...")
                await self.cmd_node.write_value(ua.Variant(3, ua.VariantType.Int16))
                await self.exec_node.write_value(True)
                await asyncio.sleep(1.5) 
                
                # 4. Transition Pathing Sequence (Cmd 4 & Cmd 5)
                await self.exec_node.write_value(False)
                await asyncio.sleep(0.5) 
                await self.cmd_node.write_value(ua.Variant(4, ua.VariantType.Int16))
                await self.exec_node.write_value(True)
                await asyncio.sleep(2.0) 
                
                await self.exec_node.write_value(False)
                await asyncio.sleep(0.5)
                await self.cmd_node.write_value(ua.Variant(5, ua.VariantType.Int16))
                await self.exec_node.write_value(True)
                await asyncio.sleep(2.0)
                
                # 5. Release Object
                print(f"[{self.station_id}] Releasing Gripper...")
                await self.gripper_node.write_value(False)
                await asyncio.sleep(1.5) # Drop buffer
                
                # 6. Return Pathing Sequence (Cmd 4 Return)
                await self.exec_node.write_value(False)
                await asyncio.sleep(0.5)
                await self.cmd_node.write_value(ua.Variant(4, ua.VariantType.Int16))
                await self.exec_node.write_value(True)
                await asyncio.sleep(1.0)
                
                # 7. Complete Execution
                await self.exec_node.write_value(False)
                await self.done_node.write_value(True)
                print(f"[{self.station_id}] Sequence Complete.")

                # Lock the sequence out from immediately re-triggering on the same box
                self.waiting_for_pickup = False

            elif not box_is_present:
                # Keep speed aligned to the actual running state.
                await self.conveyor_running.write_value(True)
                await self.conveyor_speed.write_value(ua.Variant(1.0, ua.VariantType.Float))

                current_running = await self.conveyor_running.get_value()
                current_speed = await self.conveyor_speed.get_value()
                await self.publish_mqtt(current_running, current_speed)
                self.waiting_for_pickup = True

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

    try:
        async with MqttClient("localhost") as mqtt_client, server:
            for s_id in station_ids:
                controller = ProductionLineController(s_id, idx, factory_object, mqtt_client)
                await controller.initialize_nodes()
                controllers.append(controller)

            print(f"\n[INFO] Unified OPC UA + MQTT Gateway Environment Online!")

            # Start the cyclical logic for each production line controller
            tasks = [controller.run_cyclical_logic() for controller in controllers]
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