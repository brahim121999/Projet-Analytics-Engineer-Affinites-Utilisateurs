-- Nettoyage des utilisateurs
select
    user_id,
    gender,
    status,
    is_optim
from raw_users
