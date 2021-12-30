from enum import Enum


class Inverters(str, Enum):
    voltage_dc = "voltage-dc"
    current_dc = "current-dc"
    power_dc = "power-dc"
    power_ac = "power-ac"


class Aggregations(str, Enum):
    D = "date"
    # W = "week"
    MS = "month"
    YS = "year"


class Yields(str, Enum):
    yield_reference = "reference"
    yield_absolute = "system"
    yield_final = "final"


class PerformanceRatios(str, Enum):
    performance_ratio = "performance-ratio"


class Efficiencies(str, Enum):
    efficiency_array = "array"
    efficiency_system = "system"


class Energies(str, Enum):
    energy_dc = "dc"
    energy_ac = "ac"
