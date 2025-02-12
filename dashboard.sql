with sessions_with_paid_mark as (
    select
        *,
        case
            when
                medium in (
                    'cpc',
                    'cpm',
                    'cpa',
                    'youtube',
                    'cpp',
                    'tg',
                    'social'
                )
                then 1
            else 0
        end as is_paid
    from sessions
),
visitors_with_leads as (
    select
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number() over (
            partition by s.visitor_id
            order by s.is_paid desc, s.visit_date desc
        ) as rn
    from sessions_with_paid_mark as s
    left join leads as l
        on
            l.visitor_id = s.visitor_id
            and l.created_at >= s.visit_date
)
select
    source,
    medium,
    percentile_disc(0.90) within group (
        order by date_part('day', created_at - visit_date)
    ) as days_to_lead
from visitors_with_leads
where rn = 1
group by 1, 2