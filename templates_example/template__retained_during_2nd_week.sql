WITH

14 AS maturation_days,

retained_during_2nd_week AS (

    SELECT user_id
    FROM app.events
    WHERE dt >= {end_date} - INTERVAL {window_size} {granularity} - INTERVAL maturation_days DAY
        AND dt BETWEEN dt_registered + INTERVAL 8 DAY AND dt_registered + INTERVAL 14 DAY
        AND action IN ('completed_submission', 'failed_submission', 'rejected_submission')

),

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
        AND id IN retained_during_2nd_week
    {where}
    GROUP BY dt
    ORDER BY dt DESC

)

SELECT *
FROM result;
