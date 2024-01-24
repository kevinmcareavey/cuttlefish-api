import json
import math
from dataclasses import asdict, dataclass, is_dataclass
from itertools import groupby
from json import JSONEncoder, dumps, loads
from sqlite3 import connect
from uuid import uuid4

from bjoern import run
from dacite import Config, from_dict
from falcon import App, HTTP_200, HTTP_404, HTTP_503, MEDIA_JSON
from pendulum import now

from data import APPLIANCES, EXPORT_PRICES_2019_06_24, EXPORT_PRICES_2019_11_11, EXPORT_PRICES_2019_12_02, EXPORT_PRICES_UNKNOWN, IMPORT_PRICES_2019_06_24, IMPORT_PRICES_2019_11_11, IMPORT_PRICES_2019_12_02, IMPORT_PRICES_UNKNOWN

HOST = "0.0.0.0"
PORT = 8080

DURATIONS = [2, 3, 1, 8]

# IMPORT_PRICES = IMPORT_PRICES_UNKNOWN
# EXPORT_PRICES = EXPORT_PRICES_UNKNOWN

# IMPORT_PRICES = IMPORT_PRICES_2019_12_02
# EXPORT_PRICES = EXPORT_PRICES_2019_12_02

# IMPORT_PRICES = IMPORT_PRICES_2019_11_11
# EXPORT_PRICES = EXPORT_PRICES_2019_11_11

IMPORT_PRICES = IMPORT_PRICES_2019_06_24
EXPORT_PRICES = EXPORT_PRICES_2019_06_24


class PriceResource:
    def on_get(self, request, response):
        response.status = HTTP_200
        response.content_type = MEDIA_JSON
        data = [{"import_price": import_price, "export_price": export_price} for import_price, export_price in zip(IMPORT_PRICES, EXPORT_PRICES)]
        response.text = dumps(data)


# def iter_battery_tasks(battery_plan):
#     discharge_index = 1
#     charge_index = 1
#     timestep = 0
#     for battery_action, group in groupby(battery_plan):
#         duration = sum(1 for _ in group)
#         if battery_action != 0:
#             yield {"name": f"Battery discharge ({discharge_index})" if battery_action == -1 else f"Battery charge ({charge_index})", "start": timestep, "duration": duration}
#             if battery_action == -1:
#                 discharge_index += 1
#             elif battery_action == 1:
#                 charge_index += 1
#         timestep += duration
#
#
# def iter_appliance_tasks(appliance_label, appliance_plan):
#     cycle_index = 1
#     start = 0
#     for appliance_action, group in groupby(appliance_plan):
#         duration = sum(1 for _ in group)
#         if appliance_action != 0:
#             yield {"name": f"{appliance_label} ({cycle_index})", "start": start, "duration": duration}
#             cycle_index += 1
#         start += duration
#
#
# def iter_tasks(plan):
#     task_index = 1
#     for appliance_index, appliance_label in enumerate(APPLIANCES):
#         for task in iter_appliance_tasks(appliance_label, [action["appliances"][appliance_index] for action in plan]):
#             yield {"id": f"Task ({task_index})", **task}
#             task_index += 1
#     for task in iter_battery_tasks([action["battery"] for action in plan]):
#         yield {"id": f"Task ({task_index})", **task}
#         task_index += 1
#
#
# class TasksResource:
#     def __init__(self, db_path):
#         self.db_path = db_path
#
#     def on_get(self, request, response):
#         resource_uuid = request.get_param("problem")
#
#         connection = connect(self.db_path)
#         cursor = connection.cursor()
#
#         cursor.execute("PRAGMA journal_mode=WAL")
#
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS problems (
#                 problem_id INTEGER PRIMARY KEY,
#                 created_at TIMESTAMP,
#                 problem_data JSON,
#                 resource_uuid UUID,
#                 updated_at TIMESTAMP,
#                 solution_data JSON
#             )
#         """)
#
#         result = cursor.execute("SELECT solution_data FROM problems WHERE resource_uuid = ?", (resource_uuid, )).fetchone()
#
#         connection.close()
#
#         if result:
#             solution = loads(result[0]) if result[0] else None
#             if solution:
#                 response.status = HTTP_200
#                 response.content_type = MEDIA_JSON
#                 response.text = dumps(list(iter_tasks(solution)), separators=(",", ":"))
#             else:
#                 response.status = HTTP_503
#         else:
#             response.status = HTTP_404


@dataclass(frozen=True)
class BatteryParameters:
    capacity: int
    rate: float
    initial_level: int
    min_required_level: int

    def __post_init__(self):
        assert self.capacity > 0
        assert self.rate > 0
        assert 0 <= self.initial_level <= self.capacity
        assert 0 <= self.min_required_level <= self.capacity

    def __repr__(self):
        return self.__dict__.__repr__()

    def __str__(self):
        return self.__dict__.__str__()


# @dataclass(frozen=True)
# class ApplianceParameters:
#     label: str
#     duration: int
#     rate: float
#     min_required_cycles: int
#
#     def __post_init__(self):
#         assert self.duration > 0
#         assert self.rate > 0
#         assert self.min_required_cycles >= 0
#
#     def __repr__(self):
#         return self.__dict__.__repr__()
#
#     def __str__(self):
#         return self.__dict__.__str__()
#
#
# @dataclass(frozen=True)
# class HomeParameters:
#     horizon: int
#     battery: BatteryParameters
#     appliances: tuple[ApplianceParameters, ...]
#
#     def __post_init__(self):
#         assert self.horizon > 0
#
#     def __repr__(self):
#         return self.__dict__.__repr__()
#
#     def __str__(self):
#         return self.__dict__.__str__()


@dataclass(frozen=True)
class WindowParameters:
    timesteps: set[int]
    min_required_cycles: int

    def __post_init__(self):
        assert self.min_required_cycles >= 0


@dataclass(frozen=True)
class ApplianceParameters:
    label: str
    duration: int
    rate: float
    min_required_cycles: tuple[WindowParameters, ...]
    dependencies: tuple[int | None, ...]

    def __post_init__(self):
        assert self.duration > 0
        assert self.rate > 0
        assert all(window_parameters_i.timesteps.isdisjoint(window_parameters_j.timesteps) for i, window_parameters_i in enumerate(self.min_required_cycles) for j, window_parameters_j in enumerate(self.min_required_cycles) if i != j)
        assert all(dependency is None or 0 < dependency < math.inf for dependency in self.dependencies)

    def __repr__(self):
        return self.__dict__.__repr__()

    def __str__(self):
        return self.__dict__.__str__()


@dataclass(frozen=True)
class HomeParameters:
    horizon: int
    battery: BatteryParameters
    appliances: tuple[ApplianceParameters, ...]

    def __post_init__(self):
        assert self.horizon > 0

    def __repr__(self):
        return self.__dict__.__repr__()

    def __str__(self):
        return self.__dict__.__str__()


class HomeParametersEncoder(JSONEncoder):
    def default(self, obj):
        if is_dataclass(obj):
            return asdict(obj)
        if isinstance(obj, set):
            return sorted(obj)
        return super().default(obj)


class ScheduleResource:
    def __init__(self, db_path):
        self.db_path = db_path

    def on_get(self, request, response):
        resource_uuid = request.get_param("problem")

        connection = connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("PRAGMA journal_mode=WAL")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS problems (
                problem_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP,
                problem_data JSON,
                resource_uuid UUID,
                updated_at TIMESTAMP,
                solution_data JSON
            )
        """)

        result = cursor.execute("SELECT solution_data FROM problems WHERE resource_uuid = ?", (resource_uuid, )).fetchone()

        connection.close()

        if result:
            solution = loads(result[0]) if result[0] else None
            if solution:
                response.status = HTTP_200
                response.content_type = MEDIA_JSON
                response.text = dumps(solution, separators=(",", ":"))
            else:
                response.status = HTTP_503
        else:
            response.status = HTTP_404

    def on_put(self, request, response):
        home_parameters = from_dict(data_class=HomeParameters, data=request.media, config=Config(cast=[tuple]))

        connection = connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("PRAGMA journal_mode=WAL")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS problems (
                problem_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP,
                problem_data JSON,
                resource_uuid UUID,
                updated_at TIMESTAMP,
                solution_data JSON
            )
        """)

        result = cursor.execute("SELECT problem_id, resource_uuid FROM problems WHERE problem_data = ?", (dumps(request.media, separators=(",", ":")), )).fetchone()

        if result is None:
            resource_uuid = str(uuid4())
            data = now().to_iso8601_string(), dumps(home_parameters, cls=HomeParametersEncoder, separators=(",", ":")), resource_uuid, None, None
            result = cursor.execute("INSERT OR IGNORE INTO problems (created_at, problem_data, resource_uuid, updated_at, solution_data) VALUES (?, ?, ?, ?, ?) RETURNING problem_id, resource_uuid", data).fetchone()

        problem_id, resource_uuid = result

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                request_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP,
                problem_id INTEGER,
                FOREIGN KEY(problem_id) REFERENCES problems(problem_id)
            )
        """)

        data = now().to_iso8601_string(), problem_id
        print(f"INSERT {data}")
        cursor.execute("INSERT INTO requests (created_at, problem_id) VALUES (?, ?)", data)

        connection.commit()
        connection.close()

        response.status = HTTP_200
        response.content_type = MEDIA_JSON
        response.text = dumps({"resource": resource_uuid}, separators=(",", ":"))


class ProblemResource:
    def __init__(self, db_path):
        self.db_path = db_path

    def on_get(self, request, response):
        connection = connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("PRAGMA journal_mode=WAL")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS problems (
                problem_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP,
                problem_data JSON,
                resource_uuid UUID,
                updated_at TIMESTAMP,
                solution_data JSON
            )
        """)

        result = cursor.execute("SELECT resource_uuid FROM problems WHERE solution_data IS NOT NULL").fetchall()

        connection.close()

        if result:
            response.status = HTTP_200
            response.content_type = MEDIA_JSON
            response.text = dumps([row[0] for row in result], separators=(",", ":"))
        else:
            response.status = HTTP_404


def iter_appliance_tasks(appliance_label, appliance_plan, cycle_duration):
    timestep = 0
    for appliance_action, group in groupby(appliance_plan):
        action_duration = sum(1 for _ in group)
        if appliance_action != 0:
            assert action_duration % cycle_duration == 0, f"{action_duration} % {cycle_duration} == 0 <=> {action_duration % cycle_duration} == 0"
            for _ in range(action_duration // cycle_duration):
                yield {"device": appliance_label, "action": "On", "start": timestep, "duration": cycle_duration}
                timestep += cycle_duration
        else:
            timestep += action_duration


def iter_battery_tasks(battery_plan):
    timestep = 0
    for battery_action, group in groupby(battery_plan):
        duration = sum(1 for _ in group)
        if battery_action != 0:
            yield {"device": "Battery", "action": "Discharge" if battery_action == -1 else "Charge", "start": timestep, "duration": duration}
        timestep += duration


def iter_tasks(plan):
    for appliance_index, appliance_label in enumerate(APPLIANCES):
        yield from iter_appliance_tasks(appliance_label, [action["appliances"][appliance_index] for action in plan], DURATIONS[appliance_index])
    yield from iter_battery_tasks([action["battery"] for action in plan])


class TasksResource:
    def __init__(self, db_path):
        self.db_path = db_path

    def on_get(self, request, response, problem_id):
        connection = connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("PRAGMA journal_mode=WAL")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS problems (
                problem_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP,
                problem_data JSON,
                resource_uuid UUID,
                updated_at TIMESTAMP,
                solution_data JSON
            )
        """)

        result = cursor.execute("SELECT solution_data FROM problems WHERE resource_uuid = ?", (problem_id, )).fetchone()

        connection.close()

        if result:
            solution = loads(result[0]) if result[0] else None
            if solution:
                response.status = HTTP_200
                response.content_type = MEDIA_JSON
                response.text = dumps(list(iter_tasks(solution)), separators=(",", ":"))
            else:
                response.status = HTTP_503
        else:
            response.status = HTTP_404


class RequirementsResource:
    def __init__(self, db_path):
        self.db_path = db_path

    def on_post(self, request, response):
        home_parameters = from_dict(data_class=HomeParameters, data=request.media, config=Config(cast=[tuple, set]))

        connection = connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("PRAGMA journal_mode=WAL")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS problems (
                problem_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP,
                problem_data JSON,
                resource_uuid UUID,
                updated_at TIMESTAMP,
                solution_data JSON
            )
        """)

        result = cursor.execute("SELECT problem_id, resource_uuid FROM problems WHERE problem_data = ?", (dumps(request.media, separators=(",", ":")), )).fetchone()

        if result is None:
            resource_uuid = str(uuid4())
            data = now().to_iso8601_string(), dumps(home_parameters, cls=HomeParametersEncoder, separators=(",", ":")), resource_uuid, None, None
            result = cursor.execute("INSERT OR IGNORE INTO problems (created_at, problem_data, resource_uuid, updated_at, solution_data) VALUES (?, ?, ?, ?, ?) RETURNING problem_id, resource_uuid", data).fetchone()

        problem_id, resource_uuid = result

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                request_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP,
                problem_id INTEGER,
                FOREIGN KEY(problem_id) REFERENCES problems(problem_id)
            )
        """)

        data = now().to_iso8601_string(), problem_id
        print(f"INSERT {data}")
        cursor.execute("INSERT INTO requests (created_at, problem_id) VALUES (?, ?)", data)

        connection.commit()
        connection.close()

        response.status = HTTP_200
        response.content_type = MEDIA_JSON
        response.text = dumps({"resource": resource_uuid}, separators=(",", ":"))


if __name__ == "__main__":
    app = App(cors_enable=True)
    app.req_options.strip_url_path_trailing_slash = True

    app.add_route("/prices", PriceResource())
    app.add_route("/schedule", ScheduleResource("shared.db"))
    # app.add_route("/tasks", TasksResource("shared.db"))
    app.add_route("/problems", ProblemResource("shared.db"))
    app.add_route("/tasks/{problem_id}", TasksResource("shared.db"))
    app.add_route("/requirements", RequirementsResource("shared.db"))

    print(f"Listening on http://{HOST}:{PORT}")
    run(app, HOST, PORT)
