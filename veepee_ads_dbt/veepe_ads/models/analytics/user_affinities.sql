-- Thématiques préférées des utilisateurs
with events as (
    select * from {{ ref('int_user_events') }}
),
campaigns as (
    select * from {{ ref('int_campaigns_with_themes') }}
),
joined as (
    select
        e.user_id,
        c.theme
    from events e
    join campaigns c on e.campaign_id = c.campaign_id
),
aggregated as (
    select
        user_id,
        theme,
        count(*) as event_count
    from joined
    group by user_id, theme
)

select * from aggregated
