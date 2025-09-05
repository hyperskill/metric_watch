WITH

1 AS maturation_hours,

result AS (

    SELECT
        arrayJoin(
            arrayMap(
                i -> {end_date} - INTERVAL i {granularity} - INTERVAL maturation_hours HOUR,
                range(1, {window_size} + 1)
            )
        ) AS dt,
        {slice},
        uniqExactIf(
            (user_id AS id),
            dt_joined BETWEEN dt AND dt + INTERVAL 1 {granularity}
        ) AS metric
    FROM app.events
    WHERE date BETWEEN {end_date} - INTERVAL {window_size} {granularity} - INTERVAL maturation_hours HOUR AND {end_date} - INTERVAL maturation_hours HOUR
        AND action = 'view'
        AND registration_date < '2001-01-01'
        AND dt_joined BETWEEN {end_date} - INTERVAL {window_size} {granularity} - INTERVAL maturation_hours HOUR AND {end_date} - INTERVAL maturation_hours HOUR
    {where}
    GROUP BY dt
    ORDER BY dt DESC

)

SELECT *
FROM result;
