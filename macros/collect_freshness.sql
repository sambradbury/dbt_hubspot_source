{% macro collect_freshness(source, loaded_at_field, filter, is_enabled=true) %}
  {{ return(adapter.dispatch('collect_freshness')(source, loaded_at_field, filter, is_source_enabled))}}
{% endmacro %}


{% macro default__collect_freshness(source, loaded_at_field, filter, is_enabled) %}
  {% call statement('collect_freshness', fetch_result=True, auto_begin=False) -%}
    select
      {% if is_enabled %}
      max({{ loaded_at_field }}) 
      {% else %} {{ current_timestamp() }} {% endif %} as max_loaded_at,
      {{ current_timestamp() }} as snapshotted_at
    {% if is_enabled %}
    
    from {{ source }}
      {% if filter %}
      where {{ filter }}
      {% endif %}
    {% endif %}

  {% endcall %}
  {{ return(load_result('collect_freshness').table) }}
{% endmacro %}