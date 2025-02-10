WITH paid_sessions AS (
    SELECT
        visitor_id,
        visit_date::DATE AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY visitor_id
            ORDER BY visit_date DESC
        ) AS rn
    FROM sessions
    WHERE medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
last_paid_click AS (
    SELECT
        ps.visitor_id,
        ps.visit_date,
        ps.utm_source,
        ps.utm_medium,
        ps.utm_campaign
    FROM paid_sessions ps
    LEFT JOIN leads l
    ON ps.visitor_id = l.visitor_id
    WHERE ps.rn = 1 AND (ps.visit_date <= l.created_at OR l.created_at IS NULL)
),
aggregated_data AS (
    SELECT
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        COUNT(DISTINCT lpc.visitor_id) AS visitors_count,
        SUM(CASE WHEN l.lead_id IS NOT NULL THEN 1 ELSE 0 END) AS leads_count,
        SUM(CASE WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN 1 ELSE 0 END) AS purchases_count,
        SUM(CASE WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN l.amount ELSE 0 END) AS revenue
    FROM last_paid_click lpc
    LEFT JOIN leads l
    ON lpc.visitor_id = l.visitor_id
    AND lpc.visit_date <= DATE(l.created_at)
    GROUP BY lpc.visit_date, lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
),
ad_spend AS (
    SELECT
        campaign_date::DATE AS spend_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY campaign_date, utm_source, utm_medium, utm_campaign

    UNION ALL

    SELECT
        campaign_date::DATE AS spend_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM ya_ads
    GROUP BY campaign_date, utm_source, utm_medium, utm_campaign
),
final_data AS (
    SELECT
        ad.spend_date AS visit_date,
        ad.utm_source,
        ad.utm_medium,
        ad.utm_campaign,
        COALESCE(ag.visitors_count, 0) AS visitors_count,
        COALESCE(ad.total_cost, 0) AS total_cost,
        COALESCE(ag.leads_count, 0) AS leads_count,
        COALESCE(ag.purchases_count, 0) AS purchases_count,
        COALESCE(ag.revenue, 0) AS revenue
    FROM ad_spend ad
    LEFT JOIN aggregated_data ag
    ON ad.spend_date = ag.visit_date
    AND ad.utm_source = ag.utm_source
    AND ad.utm_medium = ag.utm_medium
    AND ad.utm_campaign = ag.utm_campaign
)
SELECT
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    visitors_count,
    total_cost,
    leads_count,
    purchases_count,
    revenue
FROM final_data
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 15;