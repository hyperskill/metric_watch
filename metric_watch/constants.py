PATH_TO_METRIC_HIERARCHY = "metrics_hierarchy.yml"
TEMPLATE_GROUPBY_PART = """GROUP BY dt
    ORDER BY dt DESC"""
TEMPLATE_GROUPBY_SLICE_PART = """GROUP BY dt, {slice}
    ORDER BY dt DESC, {slice} ASC"""
TEMPLATE_PART_COUNTRY = """
        dictGetString(
            'app.countries_dict',
            'name',
            toUInt64(
                dictGet(
                    'app.users_dict',
                    'country_id',
                    toUInt64(id)
                )
            )
        ) AS country,
"""
TEMPLATE_PART_PLATFORM = """
        dictGetString(
            'app.platforms_dict',
            'platform',
            toUInt64(id)
        ) AS platform,
"""
METRIC_WATCH_DB = "metric_watch"
REPORTS_DB = "reports"
MODEL_NAME_TEMPLATE = "{name}__by_{granularity}_{window_size}__{slice}__{end_date}"
SELECT_ALL_QUERY = f"SELECT * FROM {REPORTS_DB}." + MODEL_NAME_TEMPLATE
FORMULA_TEMPLATE = f"""
WITH

numerator AS ({SELECT_ALL_QUERY}),

denominator AS ({SELECT_ALL_QUERY}),

result AS (

    SELECT
        numerator.dt,
        numerator.metric / denominator.metric AS metric
    FROM numerator
    
    JOIN denominator
        USING (dt)

)

SELECT *
FROM result;
"""
CREATE_OR_REPLACE_VIEW_AS = (
    f"CREATE OR REPLACE VIEW {REPORTS_DB}.{MODEL_NAME_TEMPLATE}\nAS\n"
)
MUTAGENS = ["start", "end", "granularity", "window"]
FILTERS = ["metric", "slice"]
LOGS = "logs"
