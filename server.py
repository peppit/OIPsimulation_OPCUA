import asyncio
import logging
import time
from asyncua import Server, ua
from asyncua.server.user_managers import UserManager

logging.basicConfig(level=logging.INFO)
logging.getLogger("asyncua.server.address_space").setLevel(logging.WARNING)
logging.getLogger("asyncua.server.standard_address_space").setLevel(logging.WARNING)


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

    robot_object = await factory_object.add_object(idx, "Robot")
    conveyor_object = await factory_object.add_object(idx, "ConveyorBelt")

    cmd_node = await robot_object.add_variable(idx, "Command", "Point1", varianttype=ua.VariantType.String)
    exec_node = await robot_object.add_variable(idx, "Execute", False, varianttype=ua.VariantType.Boolean)
    done_node = await robot_object.add_variable(idx, "Done", False, varianttype=ua.VariantType.Boolean)
    gripper_node = await robot_object.add_variable(idx, "GripperState", False, varianttype=ua.VariantType.Boolean)

    # Conveyor belt nodes
    conveyor_running = await conveyor_object.add_variable(idx, "Running", False, varianttype=ua.VariantType.Boolean)
    conveyor_speed = await conveyor_object.add_variable(idx, "Speed", 0.0, varianttype=ua.VariantType.Float)
    sensor_node = await conveyor_object.add_variable(idx, "LaserSensor", 0.0, varianttype=ua.VariantType.Float)

    await cmd_node.set_writable()
    await exec_node.set_writable()
    await done_node.set_writable()
    await gripper_node.set_writable()
    await conveyor_running.set_writable()
    await conveyor_speed.set_writable()
    await sensor_node.set_writable()

    print(f"\n[INFO] OPC UA Server started successfully!")
    print("Press Ctrl+C to shut down the server.")

    async with server:
        print("[DIAGNOSTIC] Monitoring Laser Sensor...")
        waiting_for_pick = False
        pick_end_time = 0.0
        
        while True:
            await asyncio.sleep(0.05)

            # This now reads a raw distance value (Float) instead of a True/False flag!
            current_distance = await sensor_node.get_value()
            current_time = time.time()

            # TRIGGER RULE: 
            # If the background wall is normally 2.0 meters away, a box passing by 
            # will drop that distance significantly (e.g., less than 0.5 meters).
            # (Adjust '0.5' to match whatever your sensor distance shows when blocked!)
            box_is_present = (current_distance > 0.01) and (current_distance < 0.5)

            if box_is_present and not waiting_for_pick:
                waiting_for_pick = True
                pick_end_time = current_time + 5.0
                print(f"[CONVEYOR] Box detected at distance {current_distance:.2f}m! Stopping belt...")

            if waiting_for_pick:
                if current_time >= pick_end_time:
                    waiting_for_pick = False
                    print("[CONVEYOR] Pick finished. Resuming.")
                else:
                    await conveyor_running.write_value(False)
                    await conveyor_speed.write_value(ua.Variant(0.0, ua.VariantType.Float))
            else: 
                await conveyor_running.write_value(True)
                await conveyor_speed.write_value(ua.Variant(1.0, ua.VariantType.Float))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Server stopped by user.")