{{ config(enabled=(var('hubspot_marketing_enabled', true) and var('hubspot_contact_merge_audit_enabled', false))) }}

{# TODO: figure out how to parse the var('contact_merge_audit') using something like db,schema,table= var('myvar').split('.') #}

{# {%- set source_relation = adapter.get_relation(
      database=var('contact_merge_audit').database,
      schema=var('contact_merge_audit').schema,
      identifier=var('contact_merge_audit').name) -%}

{% if source_relation is not none %}

select *
from {{ var('contact_merge_audit') }}

{% else %} #}

select
    _fivetran_synced,
    id  as canonical_vid,
    split_part(unnest(string_to_array(property_hs_calculated_merged_vids, ';')), ':', 1)::bigint  as contact_id,
    split_part(unnest(string_to_array(property_hs_calculated_merged_vids, ';')), ':', 1)::bigint  as vid_to_merge

from {{ var('contact') }}

where property_hs_calculated_merged_vids is not null

{# {% endif %} #}
