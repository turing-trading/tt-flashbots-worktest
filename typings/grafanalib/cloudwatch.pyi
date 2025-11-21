from _typeshed import Incomplete
from grafanalib.core import Target as Target

class CloudwatchMetricsTarget(Target):
    alias: Incomplete
    dimensions: Incomplete
    expression: Incomplete
    id: Incomplete
    matchExact: Incomplete
    metricName: Incomplete
    namespace: Incomplete
    period: Incomplete
    refId: Incomplete
    region: Incomplete
    statistics: Incomplete
    statistic: Incomplete
    hide: Incomplete
    datasource: Incomplete
    def to_json_data(self): ...
    def __init__(self, expr, format, legendFormat, interval, intervalFactor, metric, step, target, instant, alias, dimensions, expression, id, matchExact, metricName, namespace, period, refId, region, statistics, statistic, hide, datasource) -> None: ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...

class CloudwatchLogsInsightsTarget(Target):
    expression: Incomplete
    id: Incomplete
    logGroupNames: Incomplete
    namespace: Incomplete
    refId: Incomplete
    region: Incomplete
    statsGroups: Incomplete
    hide: Incomplete
    datasource: Incomplete
    def to_json_data(self): ...
    def __init__(self, expr, format, legendFormat, interval, intervalFactor, metric, step, target, instant, expression, id, logGroupNames, namespace, refId, region, statsGroups, hide, datasource) -> None: ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...
