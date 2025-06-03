-- Création du niveau de granularité utilisateur / combinaison
with visits as (
    select
        v.user_id,
        c.main_sector as category,
        c.main_sub_sector as sub_category,
        c.brand_name,
        m.theme,
        'visit' as event_type,
        v.nb_events as event_count
    from {{ ref('stg_visits') }} v
    join {{ ref('stg_campaigns') }} c using (campaign_id)
    left join {{ ref('stg_thematics_mapping') }} m
        on c.main_sector = m.sector and c.main_sub_sector = m.sub_sector
),
orders as (
    select
        o.user_id,
        o.main_sector as category,
        o.main_sub_sector as sub_category,
        o.brand_name,
        m.theme,
        'order' as event_type,
        o.product_sold as event_count
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_thematics_mapping') }} m
        on o.main_sector = m.sector and o.main_sub_sector = m.sub_sector
),
combined as (
    select * from visits
    union all
    select * from orders
),
aggregated as (
    select
        user_id,
        category,
        sub_category,
        brand_name,
        theme,
        concat_ws(' / ', category, sub_category, brand_name, theme) as user_level_key,
        sum(event_count) as total_events
    from combined
    group by 1, 2, 3, 4, 5, 6
)

select *
from aggregated
