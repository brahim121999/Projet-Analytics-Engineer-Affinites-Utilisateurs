-- Nettoyage des campagnes
select
    campaign_id,
    start_datetime,
    end_datetime,
    main_sector,
    main_sub_sector,
    brand_name,
    site_country_code
from raw_campaigns
