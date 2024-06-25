from dataclasses import dataclass
from enum import Enum


class BatteryAction(Enum):
    DISCHARGE = -1
    OFF = 0
    CHARGE = 1


class ApplianceAction(Enum):
    OFF = 0
    ON = 1


@dataclass(frozen=True)
class HomeAction:
    battery: BatteryAction
    appliances: tuple[ApplianceAction, ...]
