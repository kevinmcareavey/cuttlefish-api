import math
import tomllib
from dataclasses import asdict, dataclass, is_dataclass
from itertools import groupby
from json import JSONEncoder, dumps, loads
from sqlite3 import connect
from uuid import uuid4

import dacite
from bjoern import run
from dacite import Config, from_dict
from falcon import App, HTTP_200, HTTP_404, HTTP_503, MEDIA_JSON
from falcon_auth import FalconAuthMiddleware, TokenAuthBackend
from pendulum import now

from data import APPLIANCES, EXPORT_PRICES, IMPORT_PRICES

DURATIONS = [2, 3, 1, 8]


@dataclass
class APIConfig:
    host: str
    port: int


@dataclass
class DBConfig:
    path: str


@dataclass
class Config:
    api: APIConfig
    database: DBConfig


class PriceResource:
    def on_get(self, request, response):
        response.status = HTTP_200
        response.content_type = MEDIA_JSON
        data = [{"import_price": import_price, "export_price": export_price} for import_price, export_price in zip(IMPORT_PRICES, EXPORT_PRICES)]
        response.text = dumps(data)


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


def user_validator(db_path, username, api_token):
    connection = connect(db_path)
    cursor = connection.cursor()

    cursor.execute("PRAGMA journal_mode=WAL")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            created_at TIMESTAMP,
            username TEXT UNIQUE,
            api_token UUID
        )
    """)

    result = cursor.execute("SELECT username, api_token FROM users WHERE username = ? AND api_token = ? LIMIT 1", (username, api_token)).fetchone()

    connection.close()

    return username if result and result[0] == username and result[1] == api_token else None


def add_users(db_path, api_tokens):
    connection = connect(db_path)
    cursor = connection.cursor()

    cursor.execute("PRAGMA journal_mode=WAL")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            created_at TIMESTAMP,
            username TEXT UNIQUE,
            api_token UUID
        )
    """)

    def iter_insert_rows():
        for username, api_token in api_tokens.items():
            yield now().to_iso8601_string(), username, api_token

    cursor.executemany("INSERT OR IGNORE INTO users (created_at, username, api_token) VALUES (?, ?, ?)", iter_insert_rows())

    connection.commit()
    connection.close()


def add_test_users(db_path):
    api_tokens = {
        "alice": "028b6996-18be-419b-a6a2-5b14acca0418",
        "bob": "6899a98d-4406-4143-8431-dc0025d6a568",
        "carol": "b295ff45-fa84-440e-ba0d-2ae6e7acca64",
    }
    add_users(db_path, api_tokens)


if __name__ == "__main__":
    config_path = "config.toml"

    with open(config_path, "rb") as toml_file:
        toml_data = tomllib.load(toml_file)

    config = dacite.from_dict(Config, toml_data)

    add_test_users(config.database.path)

    def user_loader(bearer_token):
        tokens = [token.strip() for token in bearer_token.split(",")]
        if len(tokens) == 2:
            username, api_token = tokens
            return user_validator(config.database.path, username, api_token)
        return None

    auth_backend = TokenAuthBackend(user_loader, auth_header_prefix="Bearer")
    auth_middleware = FalconAuthMiddleware(auth_backend)

    app = App(middleware=[auth_middleware], cors_enable=True)
    app.req_options.strip_url_path_trailing_slash = True

    app.add_route("/prices", PriceResource())
    app.add_route("/schedule", ScheduleResource(config.database.path))
    # app.add_route("/tasks", TasksResource(config.database.path))
    app.add_route("/problems", ProblemResource(config.database.path))
    app.add_route("/tasks/{problem_id}", TasksResource(config.database.path))
    app.add_route("/requirements", RequirementsResource(config.database.path))

    print(f"Listening on http://{config.api.host}:{config.api.port}")
    run(app, config.api.host, config.api.port)
