import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from server import ProductionLineController, mqtt_operation_listener


class FakeNode:
    def __init__(self, value=None):
        self.value = value
        self.writes = []

    async def write_value(self, value):
        self.value = getattr(value, "Value", value)
        self.writes.append(self.value)

    async def get_value(self):
        return self.value


class FakeMessages:
    def __init__(self, messages):
        self._messages = messages

    def __aiter__(self):
        self._iterator = iter(self._messages)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration:
            raise StopAsyncIteration


class FakeMqtt:
    def __init__(self, messages=()):
        self.messages = FakeMessages(messages)
        self.subscriptions = []
        self.published = []

    async def subscribe(self, topic):
        self.subscriptions.append(topic)

    async def publish(self, topic, payload, **kwargs):
        self.published.append((topic, payload, kwargs))


def operation_payload(request_id="request-123", station_id="Station_01", robot_id=None):
    payload = {
        "requestId": request_id,
        "runId": "fault-test-01",
        "stationId": station_id,
        "operation": "moveBox",
        "params": {
            "SourcePosition": "Conveyor1",
            "TargetPosition": "Pallet1",
        },
    }
    if robot_id is not None:
        payload["robotId"] = robot_id
    return payload


class SharedRobotRoutingTests(unittest.IsolatedAsyncioTestCase):
    def make_controller(self, station_id, robot_id, mqtt=None):
        mqtt = mqtt or FakeMqtt()
        with patch.object(ProductionLineController, "_ensure_log_file_header"):
            controller = ProductionLineController(
                station_id,
                namespace_idx=1,
                idx_folder=None,
                mqtt_client=mqtt,
                server_instance_id="test-server",
                robot_id=robot_id,
            )
        controller.cmd_node = FakeNode()
        controller.exec_node = FakeNode(False)
        controller.done_node = FakeNode(True)
        controller.gripper_node = FakeNode(False)
        controller.conveyor_running = FakeNode(False)
        controller.conveyor_speed = FakeNode(0.0)
        controller.robot_ready.set()
        controller.robot_sequences = {
            "moveBox": {
                ("Conveyor1", "Pallet1"): [
                    {"action": "set_gripper", "value": True},
                ],
            },
            "moveToHome": [],
        }
        controller.pending_box_pick = True
        controller.box_is_present = False
        controller._capture_t4_and_log_pair = AsyncMock()
        return controller

    async def route_one(self, topic, payload, station_01, station_02, robots=None):
        message = SimpleNamespace(topic=topic, payload=json.dumps(payload).encode())
        mqtt = FakeMqtt([message])
        station_01.mqtt = mqtt
        station_02.mqtt = mqtt
        await mqtt_operation_listener(
            mqtt,
            {"Station_01": station_01, "Station_02": station_02},
            controllers_by_robot=robots
            or {"Robot_01": station_01, "Robot_02": station_02},
        )
        return mqtt

    async def dispatch_queued(self, controller):
        operation, payload, executor = await controller.operation_queue.get()
        try:
            await controller.handle_operation_message(operation, payload, executor)
        finally:
            controller.operation_queue.task_done()
        return executor

    async def test_local_robot_executes_local_station_request(self):
        station_01 = self.make_controller("Station_01", "Robot_01")

        await station_01.dispatch_operation("moveBox", operation_payload(), station_01)

        self.assertIn(True, station_01.gripper_node.writes)

    async def test_robot02_executes_station01_request(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")

        mqtt = await self.route_one(
            "simulation/robots/Robot_02/operations/moveBox",
            operation_payload(robot_id="Robot_02"),
            station_01,
            station_02,
        )
        executor = await self.dispatch_queued(station_01)

        self.assertIs(executor, station_02)
        self.assertIn(True, station_02.gripper_node.writes)
        moving = [
            (topic, json.loads(payload)["isMoving"])
            for topic, payload, _ in mqtt.published
            if topic.endswith("/isMoving")
        ]
        self.assertIn(("simulation/Station_02/isMoving", True), moving)
        self.assertNotIn(("simulation/Station_01/isMoving", True), moving)

    async def test_substitution_uses_robot02_opcua_nodes(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")

        await station_01.dispatch_operation("moveBox", operation_payload(), station_02)

        self.assertEqual(station_02.gripper_node.writes, [True])
        self.assertEqual(station_02.exec_node.writes, [False])

    async def test_substitution_does_not_use_robot01_opcua_nodes(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")

        await station_01.dispatch_operation("moveBox", operation_payload(), station_02)

        self.assertEqual(station_01.gripper_node.writes, [])
        self.assertEqual(station_01.exec_node.writes, [])

    async def test_station01_box_state_is_consumed_by_robot02_operation(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")

        await station_01.dispatch_operation("moveBox", operation_payload(), station_02)

        self.assertFalse(station_01.pending_box_pick)
        self.assertTrue(station_02.pending_box_pick)
        station_01._capture_t4_and_log_pair.assert_awaited_once_with(
            "request-123", "fault-test-01"
        )

    async def test_station01_reply_contains_robot02_identity(self):
        mqtt = FakeMqtt()
        station_01 = self.make_controller("Station_01", "Robot_01", mqtt)
        station_02 = self.make_controller("Station_02", "Robot_02", mqtt)

        await station_01.dispatch_operation("moveBox", operation_payload(), station_02)

        completed = [
            json.loads(payload)
            for topic, payload, _ in mqtt.published
            if topic == "simulation/Station_01/replies/moveBox"
            and json.loads(payload)["status"] == "completed"
        ]
        self.assertEqual(completed[0]["stationId"], "Station_01")
        self.assertEqual(completed[0]["robotId"], "Robot_02")

    async def test_robot02_lock_is_shared_between_both_stations(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")
        active = 0
        max_active = 0

        async def execute(_sequence):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1

        station_02._execute_robot_sequence = execute
        await asyncio.gather(
            station_01.dispatch_operation("moveBox", operation_payload("request-1"), station_02),
            station_02.dispatch_operation(
                "moveBox",
                operation_payload("request-2", station_id="Station_02"),
                station_02,
            ),
        )

        self.assertEqual(max_active, 1)
        self.assertFalse(station_02.robot_lock.locked())

    async def test_unknown_robot_is_rejected(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")

        mqtt = await self.route_one(
            "simulation/robots/Robot_99/operations/moveBox",
            operation_payload(robot_id="Robot_99"),
            station_01,
            station_02,
        )

        self.assertTrue(station_01.operation_queue.empty())
        failure = json.loads(mqtt.published[-1][1])
        self.assertEqual(failure["status"], "failed")
        self.assertIn("Unknown robot", failure["error"])

    async def test_unknown_destination_station_is_rejected(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")

        await self.route_one(
            "simulation/robots/Robot_02/operations/moveBox",
            operation_payload(station_id="Station_99", robot_id="Robot_02"),
            station_01,
            station_02,
        )

        self.assertTrue(station_01.operation_queue.empty())
        self.assertTrue(station_02.operation_queue.empty())

    async def test_topic_payload_robot_mismatch_is_rejected(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")

        mqtt = await self.route_one(
            "simulation/robots/Robot_02/operations/moveBox",
            operation_payload(robot_id="Robot_01"),
            station_01,
            station_02,
        )

        self.assertTrue(station_01.operation_queue.empty())
        failure = json.loads(mqtt.published[-1][1])
        self.assertIn("does not match", failure["error"])

    async def test_robot_route_requires_destination_station(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")
        payload = operation_payload(robot_id="Robot_02")
        del payload["stationId"]

        await self.route_one(
            "simulation/robots/Robot_02/operations/moveBox",
            payload,
            station_01,
            station_02,
        )

        self.assertTrue(station_01.operation_queue.empty())
        self.assertTrue(station_02.operation_queue.empty())

    async def test_selected_robot_readiness_is_checked(self):
        mqtt = FakeMqtt()
        station_01 = self.make_controller("Station_01", "Robot_01", mqtt)
        station_02 = self.make_controller("Station_02", "Robot_02", mqtt)
        station_02.robot_ready.clear()

        await station_01.dispatch_operation("moveBox", operation_payload(), station_02)

        self.assertTrue(station_01.pending_box_pick)
        self.assertEqual(station_02.gripper_node.writes, [])
        failure = json.loads(mqtt.published[-1][1])
        self.assertEqual(failure["robotId"], "Robot_02")
        self.assertIn("not ready", failure["error"])

    async def test_legacy_station_operation_topic_still_works(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")

        mqtt = await self.route_one(
            "simulation/Station_01/operations/moveBox",
            operation_payload(),
            station_01,
            station_02,
        )
        executor = await self.dispatch_queued(station_01)

        self.assertIs(executor, station_01)
        self.assertIn("simulation/+/operations/+", mqtt.subscriptions)
        self.assertIn(True, station_01.gripper_node.writes)

    async def test_conveyor_operations_remain_station_routed(self):
        station_01 = self.make_controller("Station_01", "Robot_01")
        station_02 = self.make_controller("Station_02", "Robot_02")
        payload = {
            "requestId": "conveyor-request",
            "stationId": "Station_02",
            "operation": "conveyorSpeed",
            "params": {"value": 2.5},
        }

        await self.route_one(
            "simulation/Station_02/operations/conveyorSpeed",
            payload,
            station_01,
            station_02,
        )
        executor = await self.dispatch_queued(station_02)

        self.assertIs(executor, station_02)
        self.assertEqual(station_02.conveyor_speed.value, 2.5)
        self.assertEqual(station_01.conveyor_speed.value, 0.0)

        rejected_mqtt = await self.route_one(
            "simulation/robots/Robot_02/operations/conveyorSpeed",
            payload,
            station_01,
            station_02,
        )
        self.assertTrue(station_02.operation_queue.empty())
        failure = json.loads(rejected_mqtt.published[-1][1])
        self.assertIn("only accept robot operations", failure["error"])


if __name__ == "__main__":
    unittest.main()
