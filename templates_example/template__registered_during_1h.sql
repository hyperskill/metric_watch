WITH

1 AS maturation_hours,

registered_during_1h AS (

    SELECT id
    FROM app.users
    WHERE date_registered BETWEEN date_joined AND date_joined + INTERVAL maturation_hours HOUR

),

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
            id,
            date_registered BETWEEN dt AND dt + INTERVAL 1 {granularity}
        ) AS metric
    FROM app.users
    WHERE date_registered BETWEEN {end_date} - INTERVAL {window_size} {granularity} - INTERVAL maturation_hours HOUR AND {end_date} - INTERVAL maturation_hours HOUR
        AND id IN registered_during_1h
    {where}
    GROUP BY dt
    ORDER BY dt DESC

)

SELECT *
FROM result;
