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

    cmd_node = await robot_object.add_variable(idx, "Command", 1, varianttype=ua.VariantType.Int16)
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
        
        while True:
            await asyncio.sleep(0.05)

            # This now reads a raw distance value (Float) instead of a True/False flag!
            current_distance = await sensor_node.get_value()

            # TRIGGER RULE: 
            # If the background wall is normally 2.0 meters away, a box passing by 
            # will drop that distance significantly (e.g., less than 0.5 meters).
            # (Adjust '0.5' to match whatever your sensor distance shows when blocked!)
            box_is_present = (current_distance > 0.01) and (current_distance < 0.5)
            waiting_for_pickup = True

            if box_is_present:
                # 1. Stop the conveyor
                await conveyor_running.write_value(False)
                await conveyor_speed.write_value(ua.Variant(0.0, ua.VariantType.Float))
                waiting_for_pickup = True
                await asyncio.sleep(0.5) # Give the belt time to friction-stop
                
            if waiting_for_pickup and box_is_present:
                # 2. Command: Move to Pick Position (Command 2)
                await cmd_node.write_value(ua.Variant(2, ua.VariantType.Int16))
                await exec_node.write_value(True)
                print("Moving to pick position...")
                await asyncio.sleep(2.0)

                print("Arrived! Actuating Gripper...")
                await exec_node.write_value(False) 
                await gripper_node.write_value(True)
                await asyncio.sleep(1)
                
                # 4. Command: Move to Place Position (Command 3)
                print("Moving to place position...")
                await cmd_node.write_value(ua.Variant(3, ua.VariantType.Int16))
                await exec_node.write_value(True)
                
                # How long does it take to move to the placement area?
                await asyncio.sleep(1.5) 
                await exec_node.write_value(False)
                await asyncio.sleep(0.5) 
                await cmd_node.write_value(ua.Variant(4, ua.VariantType.Int16))
                await exec_node.write_value(True)

                await asyncio.sleep(2) 
                await exec_node.write_value(False)
                await asyncio.sleep(0.5)
                await cmd_node.write_value(ua.Variant(5, ua.VariantType.Int16))
                await exec_node.write_value(True)

                await asyncio.sleep(2)
                await gripper_node.write_value(False)
                
                await asyncio.sleep(1.5) # Give the physics engine a second to release the box
                await exec_node.write_value(False)
                await asyncio.sleep(0.5)
                await cmd_node.write_value(ua.Variant(4, ua.VariantType.Int16))
                await exec_node.write_value(True)

                await asyncio.sleep(1.0)
                await exec_node.write_value(False)
                await done_node.write_value(True)

                # 6. Reset logic flags for the next box
                waiting_for_pickup = False

# Separate logic for the conveyor so it doesn't fight the robot
            elif not box_is_present: 
                await conveyor_running.write_value(True)
                await conveyor_speed.write_value(ua.Variant(1.0, ua.VariantType.Float))
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Server stopped by user.")