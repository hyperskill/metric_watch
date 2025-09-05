from metric_watch.database import db
from metric_watch.tables import MetricWatchLog

# Create tables if not exist
db.create_table(MetricWatchLog)
