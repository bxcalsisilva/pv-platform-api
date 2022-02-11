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
    yield_final = "dc"
    yield_absolute = "ac"


class Efficiencies(str, Enum):
    efficiency_array = "array"
    efficiency_system = "system"


class Energies(str, Enum):
    energy_dc = "dc"
    energy_ac = "ac"


class Comparations(str, Enum):
    yield_reference = "reference-yield"
    yield_final = "array-yield"
    yield_absolute = "system-yield"
    performance_ratio = "performance-ratio"
    efficiency_array = "array-efficiency"
    efficiency_system = "system-efficiency"
    efficiency_inverter = "inverter-efficiency"
    energy_dc = "dc-energy"
    energy_ac = "ac-energy"


class Technologies(str, Enum):
    PERC = "perc"
    HIT = "hit"
    CIGS = "cigs"
