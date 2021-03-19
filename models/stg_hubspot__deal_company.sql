{{ config(enabled=fivetran_utils.enabled_vars(['hubspot_sales_enabled','hubspot_deal_enabled'])) }}

with base as (

    select *
    from {{ ref('stg_hubspot__deal_company_tmp') }}

), macro as (

    select 
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_hubspot__deal_company_tmp')),
                staging_columns=get_deal_company_columns()
            )
        }}
    from base

), fields as (

    select
        company_id,
        deal_id,
        _fivetran_synced
        
    from macro
    
)

select *
from fields