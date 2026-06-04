select 
    zone_id as location_id,
    zone_name,
    borough
from {{ ref('nyc_zones') }}