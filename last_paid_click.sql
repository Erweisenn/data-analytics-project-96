WITH paid_sessions AS (
    SELECT
        visitor_id,
        visit_date,
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
    FROM paid_sessions as ps
    LEFT JOIN leads as l
    	ON ps.visitor_id = l.visitor_id
    WHERE ps.rn = 1 AND (ps.visit_date <= l.created_at OR l.created_at IS NULL)
),

final_data AS (
    SELECT
        lpc.visitor_id,
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM last_paid_click as lpc
    LEFT JOIN leads as l
		ON lpc.visitor_id = l.visitor_id
			AND lpc.visit_date <= l.created_at
)

SELECT
    visitor_id,
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM final_data
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10
