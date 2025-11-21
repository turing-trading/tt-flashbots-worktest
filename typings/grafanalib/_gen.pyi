
from json import JSONEncoder

from typing import Any

class DashboardEncoder(JSONEncoder):
    def default(self, obj: Any) -> Any: ...
