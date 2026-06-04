{% macro filter_valid_trips(min_distance=0, min_fare=0) %}
    where trip_distance > {{ min_distance }}
    and fare_amount > {{ min_fare }}
    and passenger_count > 0
{% endmacro %}