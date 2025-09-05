WITH

10 AS maturation_days,

paid_users AS (

    SELECT user_id
    FROM reports.sales
    WHERE amount_usd_cents > 0
      AND product_type IN ('personal', 'premium')
      AND user_id > 0

),

subscribed_during_10d AS (

    SELECT id
    FROM app.users
    WHERE sub_created_at_least BETWEEN date_registered AND date_registered + INTERVAL maturation_days DAY
        AND id IN paid_users

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
    WHERE date_registered BETWEEN {end_date} - INTERVAL {window_size} {granularity} - INTERVAL maturation_days DAY AND {end_date}  - INTERVAL maturation_days DAY
    AND id IN subscribed_during_10d
    {where}
    GROUP BY dt
    ORDER BY dt DESC

)

SELECT *
FROM result;
