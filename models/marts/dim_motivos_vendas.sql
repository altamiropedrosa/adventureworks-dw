with 
    stg_vendas_motivos as (
        select * from {{ ref('stg_sap_adw_vendas_motivos') }}
    )

    ,join_tables as (
        select
            id_motivo_venda
            ,nm_motivo_venda
            ,cd_motivo_razao
            ,dt_modificacao
        from stg_vendas_motivos
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_motivo_venda']) }} as sk_motivo_venda
            ,join_tables.*
        from join_tables
    )

select * from refined 