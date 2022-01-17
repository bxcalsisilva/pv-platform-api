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
    yield_final = "array"
    yield_absolute = "system"


class PerformanceRatios(str, Enum):
    yield_absolute = "ac"
    yield_final = "dc"


class Efficiencies(str, Enum):
    efficiency_array = "array"
    efficiency_system = "system"


class Energies(str, Enum):
    energy_dc = "dc"
    energy_ac = "ac"


class Comparations(str, Enum):
    yield_reference = "reference-yield"
    yield_absolute = "array-yield"
    yield_final = "system-yield"
    performance_ratio = "performance-ratio"
    efficiency_array = "array-efficiency"
    efficiency_system = "system-efficiency"
    energy_dc = "energy-dc"
    energy_ac = "energy-ac"


class Technologies(str, Enum):
    PERC = "perc"
    HIT = "hit"
    CIGS = "cigs"
