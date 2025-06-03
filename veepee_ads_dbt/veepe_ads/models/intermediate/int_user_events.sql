-- Union des visites et commandes par utilisateur
with visits as (
    select
        user_id,
        campaign_id,
        visit_date as event_date,
        'visit' as event_type
    from {{ ref('stg_visits') }}
),
orders as (
    select
        user_id,
        campaign_id,
        order_creation_date as event_date,
        'order' as event_type
    from {{ ref('stg_orders') }}
),
combined as (
    select * from visits
    union all
    select * from orders
)

select * from combined
