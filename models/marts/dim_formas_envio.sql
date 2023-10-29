/*{{
    config(
        materialized='incremental',
        unique_key='id_forma_envio'
    )
}}*/

with 
    stg_formas_envios as (
        select * from {{ ref('stg_sap_adw_formas_envios') }}
    )

    ,join_tables as (
        select
            id_forma_envio 
            ,nm_forma_envio
            ,vl_envio_minimo
            ,vl_envio_por_kg
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(dt_modificacao as timestamp)) as timestamp) as dt_modificacao
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga

        from stg_formas_envios
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_forma_envio']) }} as sk_forma_envio
            ,join_tables.*
        from join_tables
    )

select * from refined 

/*{% if is_incremental %} 
where dt_modificacao >= (select max(dt_carga) from {{ this }})
{% endif %}*/
