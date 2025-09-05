WITH

14 AS maturation_days,

result AS (

    SELECT
        arrayJoin(
            arrayMap(
                i -> {end_date} - INTERVAL i {granularity} - INTERVAL maturation_days DAY,
                range(1, {window_size} + 1)
            )
        ) AS dt,
        {slice},
        uniqExactIf(
            id,
            date_registered BETWEEN dt AND dt + INTERVAL 1 {granularity}
        ) AS metric
    FROM app.users
    WHERE date_registered BETWEEN {end_date} - INTERVAL {window_size} {granularity} - INTERVAL maturation_days DAY AND {end_date} - INTERVAL maturation_days DAY
    {where}
    GROUP BY dt
    ORDER BY dt DESC

)

SELECT *
FROM result;
