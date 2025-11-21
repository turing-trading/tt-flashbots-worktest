from _typeshed import Incomplete
from grafanalib import prometheus as prometheus

YELLOW: str
GREEN: str
BLUE: str
ORANGE: str
RED: str
ALIAS_COLORS: Incomplete

def QPSGraph(data_source, title, expressions, **kwargs): ...
def stacked(graph): ...
def PercentUnitAxis(label=None): ...
