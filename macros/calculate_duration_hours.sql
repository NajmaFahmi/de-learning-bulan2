{% macro calculate_duration_hours(column_name) %}
    ({{ column_name }} / 60)::float
{% endmacro %}