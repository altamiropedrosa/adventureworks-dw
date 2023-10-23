with 
    stg_formas_envios as (
        select * from {{ ref('stg_sap_adw_formas_envios') }}
    )

    ,join_tables as (
        select
            id_forma_envio
            ,nm_forma_envio
            ,vl_minimo_envio
            ,vl_envio_por_kg
            ,dt_modificacao
        from stg_formas_envios
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_forma_envio']) }} as sk_forma_envio
            ,join_tables.*
        from join_tables
    )

select * from refined 