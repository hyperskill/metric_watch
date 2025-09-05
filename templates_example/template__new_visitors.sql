WITH

result AS (

    SELECT
        arrayJoin(
            arrayMap(
                i -> {end_date} - INTERVAL i {granularity},
                range(1, {window_size} + 1)
            )
        ) AS dt,
        {slice},
        uniqExactIf(
            (user_id AS id),
            dt_joined BETWEEN dt AND dt + INTERVAL 1 {granularity}
        ) AS metric
    FROM app.events
    WHERE date BETWEEN {end_date} - INTERVAL {window_size} {granularity} AND {end_date}
        AND action = 'view'
        AND registration_date < '2001-01-01'  -- not registered
        AND dt_joined BETWEEN {end_date} - INTERVAL {window_size} {granularity} AND {end_date}
    {where}
    GROUP BY dt
    ORDER BY dt DESC

)

SELECT *
FROM result;
