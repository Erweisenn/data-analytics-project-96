with all_data as (
    select
        s.visitor_id,
        s.visit_date,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        s.source as utm_source,
        row_number() over (
            partition by s.visitor_id order by s.visit_date desc
        ) as rn
    from sessions as s
    left join leads as l
        on
            s.visitor_id = l.visitor_id
            and s.visit_date <= l.created_at
    where s.medium != 'organic'
),

counts as (
    select
        lower(utm_source) as utm_source,
        utm_medium,
        utm_campaign,
        visit_date::date as visit_date,
        count(visitor_id) as visitors_count,
        count(case
            when created_at is not null
                then visitor_id
        end) as leads_count,
        count(case
            when status_id = 142
                then visitor_id
        end) as purchases_count,
        sum(case when status_id = 142 then amount end) as revenue
    from all_data
    where rn = 1
    group by 1, 2, 3, 4
),

vk_total as (
    select
        campaign_date::date as campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from vk_ads
    group by 1, 2, 3, 4
),

yandex_total as (
    select
        campaign_date::date as campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from ya_ads
    group by 1, 2, 3, 4
),

totals as (
    select *
    from vk_total
    union
    select *
    from yandex_total
)

select
    c.visit_date,
    c.visitors_count,
    c.utm_source,
    c.utm_medium,
    c.utm_campaign,
    t.total_cost,
    c.leads_count,
    c.purchases_count,
    c.revenue
from counts as c
left join totals as t
    on
        c.visit_date = t.campaign_date
        and c.utm_source = t.utm_source
        and c.utm_medium = t.utm_medium
        and c.utm_campaign = t.utm_campaign
order by 9 desc nulls last, 1, 2 desc, 3, 4
limit 15