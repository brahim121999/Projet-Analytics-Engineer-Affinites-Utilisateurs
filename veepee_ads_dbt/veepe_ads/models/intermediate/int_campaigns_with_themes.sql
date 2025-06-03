-- Association des campagnes avec les thèmes
select
    c.campaign_id,
    c.main_sector as sector,
    c.main_sub_sector as sub_sector,
    c.brand_name,
    c.site_country_code,
    m.theme
from {{ ref('stg_campaigns') }} c
left join {{ ref('stg_thematics_mapping') }} m
  on c.main_sector = m.sector and c.main_sub_sector = m.sub_sector
