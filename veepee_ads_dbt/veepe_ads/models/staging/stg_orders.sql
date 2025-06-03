-- Nettoyage des commandes
select
    user_id,
    campaign_id,
    order_creation_date,
    product_booking_incl_vat,
    product_sold,
    is_cancelled,
    main_sector,
    main_sub_sector,
    brand_name,
    site_country_code
from raw_orders
where is_cancelled = false
