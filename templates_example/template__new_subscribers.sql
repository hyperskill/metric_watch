WITH

paid_users AS (

    SELECT user_id
    FROM reports.sales
    WHERE amount_usd_cents > 0
      AND product_type IN ('personal', 'premium')
      AND user_id > 0

),

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
            user_id AS id,
            created_at BETWEEN dt AND dt + INTERVAL 1 {granularity}
        ) AS metric
    FROM app.subscriptions
    WHERE
        type IN ('personal', 'premium')
        AND id IN paid_users
        AND toDate(created_at) BETWEEN {end_date} - INTERVAL {window_size} {granularity} AND {end_date}
        {where}
    GROUP BY dt
    ORDER BY dt DESC

)

SELECT *
FROM result;
