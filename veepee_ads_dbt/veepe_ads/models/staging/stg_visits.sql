-- Nettoyage des visites
select
    user_id,
    campaign_id,
    visit_date,
    banner_type,
    nb_events,
    site_country_code
from raw_visits
where user_id is not null
