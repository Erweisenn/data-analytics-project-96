WITH costs AS (
    SELECT 
        utm_source,
        SUM(daily_spent) AS total_cost
    FROM 
        (
            SELECT 
                utm_source,
                daily_spent
            FROM 
                vk_ads
            UNION ALL
            SELECT 
                utm_source,
                daily_spent
            FROM 
                ya_ads
        ) AS combined_ads
    GROUP BY 
        utm_source
),
visitors AS (
    SELECT 
        source AS utm_source,
        COUNT(DISTINCT visitor_id) AS visitors_count
    FROM 
        sessions
    GROUP BY 
        source
),
leads AS (
    SELECT 
        s.source AS utm_source,
        COUNT(DISTINCT l.lead_id) AS leads_count
    FROM 
        sessions s
    LEFT JOIN 
        leads l ON s.visitor_id = l.visitor_id
    GROUP BY 
        s.source
),
purchases AS (
    SELECT 
        s.source AS utm_source,
        COUNT(DISTINCT l.lead_id) AS purchases_count,
        SUM(l.amount) AS total_revenue
    FROM
        sessions s
    JOIN 
        leads l 
        ON s.visitor_id = l.visitor_id
    GROUP BY 
        s.source
)
SELECT 
    v.utm_source,
    v.visitors_count,
    l.leads_count,
    p.purchases_count,
    c.total_cost,
    p.total_revenue,
    (c.total_cost / v.visitors_count) AS cpu,
    (c.total_cost / l.leads_count) AS cpl,
    (c.total_cost / p.purchases_count) AS cppu,
    ((p.total_revenue - c.total_cost) / c.total_cost) * 100 AS roi
FROM 
    visitors v
JOIN 
    leads l ON v.utm_source = l.utm_source
JOIN 
    purchases p ON v.utm_source = p.utm_source
JOIN 
    costs c ON v.utm_source = c.utm_source;