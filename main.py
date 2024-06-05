import argparse
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
from falcon import App, HTTP_200, HTTP_404, HTTP_503, MEDIA_JSON, HTTP_400
from falcon_auth import FalconAuthMiddleware, TokenAuthBackend
from pendulum import now

from data import APPLIANCES, EXPORT_PRICES, IMPORT_PRICES

DURATIONS = [2, 3, 1, 8]

CREATE_TABLE_PROBLEMS = """
    CREATE TABLE IF NOT EXISTS problems (
        problem_id INTEGER PRIMARY KEY, 
        created_at TIMESTAMP,
        problem_data JSON,
        resource_uuid UUID,
        queued_at TIMESTAMP,
        result_at TIMESTAMP,
        result_status INTEGER,
        result_data JSON
    )
    """

CREATE_TABLE_REQUESTS = """
        CREATE TABLE IF NOT EXISTS requests (
            request_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            created_at TIMESTAMP,
            problem_id INTEGER,
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            FOREIGN KEY(problem_id) REFERENCES problems(problem_id)
        )
    """

CREATE_TABLE_USERS = """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            created_at TIMESTAMP,
            username TEXT UNIQUE,
            api_token UUID
        )
    """

CREATE_TABLE_SURVEY = """
        CREATE TABLE IF NOT EXISTS survey (
            response_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            created_at TIMESTAMP,
            response_data JSON,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        )
    """


@dataclass
class APIConfig:
    host: str
    port: int


@dataclass
class DBConfig:
    path: str


@dataclass
class GlobalConfig:
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

        cursor.execute(CREATE_TABLE_PROBLEMS)

        result = cursor.execute("SELECT result_data FROM problems WHERE resource_uuid = ?", (problem_id, )).fetchone()

        connection.close()

        if result:
            solution = result[0]
            if solution:
                if solution == "unsolvable":
                    response.status = HTTP_400
                else:
                    response.status = HTTP_200
                    response.content_type = MEDIA_JSON
                    response.text = dumps(list(iter_tasks(loads(solution))), separators=(",", ":"))
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

        cursor.execute(CREATE_TABLE_PROBLEMS)

        result = cursor.execute("SELECT problem_id, resource_uuid FROM problems WHERE problem_data = ?", (dumps(request.media, separators=(",", ":")), )).fetchone()

        if result is None:
            resource_uuid = str(uuid4())
            data = now().to_iso8601_string(), dumps(home_parameters, cls=HomeParametersEncoder, separators=(",", ":")), resource_uuid, None, None, None, None
            result = cursor.execute("INSERT OR IGNORE INTO problems (created_at, problem_data, resource_uuid, queued_at, result_at, result_status, result_data) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING problem_id, resource_uuid", data).fetchone()

        problem_id, resource_uuid = result

        cursor.execute(CREATE_TABLE_REQUESTS)

        data = request.context["user"]["user_id"], now().to_iso8601_string(), problem_id
        print(f"INSERT {data}")
        cursor.execute("INSERT INTO requests (user_id, created_at, problem_id) VALUES (?, ?, ?)", data)

        connection.commit()
        connection.close()

        response.status = HTTP_200
        response.content_type = MEDIA_JSON
        response.text = dumps({"resource": resource_uuid}, separators=(",", ":"))


class LoginResource:
    auth = {
        "auth_disabled": True,
    }

    def __init__(self, db_path):
        self.db_path = db_path

    def on_post(self, request, response):
        username = request.media["username"] if "username" in request.media else None

        connection = connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("PRAGMA journal_mode=WAL")

        cursor.execute(CREATE_TABLE_USERS)

        result = cursor.execute("SELECT api_token FROM users WHERE username = ? LIMIT 1", (username, )).fetchone()
        api_token = result[0] if result and len(result) > 0 else None

        if api_token is None:
            api_token = str(uuid4())
            row = now().to_iso8601_string(), username, api_token
            cursor.execute("INSERT OR IGNORE INTO users (created_at, username, api_token) VALUES (?, ?, ?)", row)

        connection.commit()
        connection.close()

        response.status = HTTP_200
        response.content_type = MEDIA_JSON
        response.text = dumps({"username": username, "token": api_token}, separators=(",", ":"))


def user_validator(db_path, username, api_token):
    connection = connect(db_path)
    cursor = connection.cursor()

    cursor.execute("PRAGMA journal_mode=WAL")

    cursor.execute(CREATE_TABLE_USERS)

    result = cursor.execute("SELECT user_id, username, api_token FROM users WHERE username = ? AND api_token = ? LIMIT 1", (username, api_token)).fetchone()

    connection.close()

    return {"user_id": result[0], "username": result[1]} if result and result[1] == username and result[2] == api_token else None


def add_test_users(db_path):
    connection = connect(db_path)
    cursor = connection.cursor()

    cursor.execute("PRAGMA journal_mode=WAL")

    cursor.execute(CREATE_TABLE_USERS)

    api_tokens = {
        "alice": "028b6996-18be-419b-a6a2-5b14acca0418",
        "bob": "6899a98d-4406-4143-8431-dc0025d6a568",
        "carol": "b295ff45-fa84-440e-ba0d-2ae6e7acca64",
    }

    def iter_insert_rows():
        for username, api_token in api_tokens.items():
            yield now().to_iso8601_string(), username, api_token

    cursor.executemany("INSERT OR IGNORE INTO users (created_at, username, api_token) VALUES (?, ?, ?)", iter_insert_rows())

    connection.commit()
    connection.close()


class SurveyResource:
    def __init__(self, db_path):
        self.db_path = db_path

    def on_post(self, request, response):
        connection = connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute("PRAGMA journal_mode=WAL")

        cursor.execute(CREATE_TABLE_SURVEY)

        data = request.context["user"]["user_id"], now().to_iso8601_string(), dumps(request.media, separators=(",", ":"))
        print(f"INSERT {data}")
        cursor.execute("INSERT INTO SURVEY (user_id, created_at, response_data) VALUES (?, ?, ?)", data)

        connection.commit()
        connection.close()

        response.status = HTTP_200


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("toml_file", metavar="TOML", type=argparse.FileType("rb"), help="load config from %(metavar)s file")

    args = parser.parse_args()
    toml_data = tomllib.load(args.toml_file)

    config = dacite.from_dict(GlobalConfig, toml_data)

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

    app.add_route("/login", LoginResource(config.database.path))
    app.add_route("/prices", PriceResource())
    app.add_route("/requirements", RequirementsResource(config.database.path))
    app.add_route("/tasks/{problem_id}", TasksResource(config.database.path))
    app.add_route("/survey", SurveyResource(config.database.path))

    print(f"Listening on http://{config.api.host}:{config.api.port}")
    run(app, config.api.host, config.api.port)
