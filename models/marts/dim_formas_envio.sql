/*{{
    config(
        materialized='incremental',
        unique_key='id_forma_envio'
    )
}}*/

with 
    stg_formas_envios as (
        select *
        from {{ ref('stg_sap_adw_formas_envio') }}
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_forma_envio']) }} as sk_forma_envio
            ,stg_formas_envios.*
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from stg_formas_envios
    )


select * from refined 

/*{% if is_incremental %} 
where dt_modificacao >= (select max(dt_carga) from {{ this }})
{% endif %}*/
