from _typeshed import Incomplete
from grafanalib.validators import is_in as is_in

OTSDB_AGG_AVG: str
OTSDB_AGG_COUNT: str
OTSDB_AGG_DEV: str
OTSDB_AGG_EP50R3: str
OTSDB_AGG_EP50R7: str
OTSDB_AGG_EP75R3: str
OTSDB_AGG_EP75R7: str
OTSDB_AGG_EP90R3: str
OTSDB_AGG_EP90R7: str
OTSDB_AGG_EP95R3: str
OTSDB_AGG_EP95R7: str
OTSDB_AGG_EP99R3: str
OTSDB_AGG_EP99R7: str
OTSDB_AGG_EP999R3: str
OTSDB_AGG_EP999R7: str
OTSDB_AGG_FIRST: str
OTSDB_AGG_LAST: str
OTSDB_AGG_MIMMIN: str
OTSDB_AGG_MIMMAX: str
OTSDB_AGG_MIN: str
OTSDB_AGG_MAX: str
OTSDB_AGG_NONE: str
OTSDB_AGG_P50: str
OTSDB_AGG_P75: str
OTSDB_AGG_P90: str
OTSDB_AGG_P95: str
OTSDB_AGG_P99: str
OTSDB_AGG_P999: str
OTSDB_AGG_SUM: str
OTSDB_AGG_ZIMSUM: str
OTSDB_DOWNSAMPLING_FILL_POLICIES: Incomplete
OTSDB_DOWNSAMPLING_FILL_POLICY_DEFAULT: str
OTSDB_QUERY_FILTERS: Incomplete
OTSDB_QUERY_FILTER_DEFAULT: str

class OpenTSDBFilter:
    value: Incomplete
    tag: Incomplete
    type: Incomplete
    groupBy: Incomplete
    def to_json_data(self): ...
    def __init__(self, value, tag, type, groupBy) -> None: ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...

class OpenTSDBTarget:
    metric: Incomplete
    refId: Incomplete
    aggregator: Incomplete
    alias: Incomplete
    isCounter: Incomplete
    counterMax: Incomplete
    counterResetValue: Incomplete
    disableDownsampling: Incomplete
    downsampleAggregator: Incomplete
    downsampleFillPolicy: Incomplete
    downsampleInterval: Incomplete
    filters: Incomplete
    shouldComputeRate: Incomplete
    currentFilterGroupBy: Incomplete
    currentFilterKey: Incomplete
    currentFilterType: Incomplete
    currentFilterValue: Incomplete
    def to_json_data(self): ...
    def __init__(self, metric, refId, aggregator, alias, isCounter, counterMax, counterResetValue, disableDownsampling, downsampleAggregator, downsampleFillPolicy, downsampleInterval, filters, shouldComputeRate, currentFilterGroupBy, currentFilterKey, currentFilterType, currentFilterValue) -> None: ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...
