WITH

1 AS maturation_hours,

activated_during_1h AS (

    SELECT user_id
    FROM app.events
    WHERE dt BETWEEN dt_registered AND dt_registered + INTERVAL maturation_hours HOUR
        AND action IN ('completed_submission', 'failed_submission', 'rejected_submission')

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
        AND id IN activated_during_1h
    {where}
    GROUP BY dt
    ORDER BY dt DESC

)

SELECT *
FROM result;
