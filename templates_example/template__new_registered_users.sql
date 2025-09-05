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
            id,
            date_registered BETWEEN dt AND dt + INTERVAL 1 {granularity}
        ) AS metric
    FROM app.users
    WHERE date_registered BETWEEN {end_date} - INTERVAL {window_size} {granularity} AND {end_date}
    {where}
    GROUP BY dt
    ORDER BY dt DESC

)

SELECT *
FROM result;
