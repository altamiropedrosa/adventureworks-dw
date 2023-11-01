with 
    stg_motivos_vendas as (
        select * from {{ ref('stg_sap_adw_motivos_vendas') }}
    )


    ,join_tables as (
        select
            id_motivo_venda
            ,nm_motivo_venda
            ,cd_motivo_venda
            ,dt_modificacao
        from stg_motivos_vendas

    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_motivo_venda']) }} as sk_motivo_venda
            ,join_tables.*
        from join_tables
    )

select * from refined 

