from infi.clickhouse_orm import (  # type: ignore
    F,
    DateTimeField,
    Model,
    StringField,
    Float32Field,
    UInt8Field,
    UInt16Field,
    NullableField,
)
from infi.clickhouse_orm.engines import MergeTree  # type: ignore


class MetricWatchLog(Model):
    dt = DateTimeField()
    is_alert = NullableField(UInt8Field())
    metric_name = NullableField(StringField())
    model_name = NullableField(StringField())
    metric_last_dt = NullableField(DateTimeField())
    granularity = NullableField(StringField())
    window_size = NullableField(UInt16Field())
    sigma_n = NullableField(UInt8Field())
    slice = NullableField(StringField())
    dist_avg = NullableField(Float32Field())
    std_dev = NullableField(Float32Field())
    metric_value = NullableField(Float32Field())
    deviation = NullableField(Float32Field())
    deviation_percent = NullableField(Float32Field())
    is_regular = NullableField(UInt8Field())
    run_type = NullableField(StringField())
    user = NullableField(StringField())

    engine = MergeTree(
        partition_key=[
            F.toStartOfYear(
                dt,
            )
        ],
        order_by=("dt",),
    )

    @classmethod
    def table_name(cls):
        return "metric_watch_log"


class Subscriptions(Model):
    user = StringField()
    metric = StringField()
    subscribed_at = DateTimeField()

    engine = MergeTree(partition_key=("user",), order_by=("subscribed_at",))
