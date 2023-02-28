{{ config(enabled=(var('hubspot_marketing_enabled', true) and var('hubspot_contact_merge_audit_enabled', false))) }}

select *
from {{ var('contact_merge_audit') }}

where property_hs_calculated_merged_vids is not null
    or id::character varying=property_hs_all_contact_vids