with 
    stg_vendas_pedidos_motivos as (
        select id_pedido_venda, id_motivo_venda
        from {{ ref('stg_sap_adw_vendas_pedidos_motivos') }}
    )
    ,stg_motivos_vendas as (
        select * from {{ ref('stg_sap_adw_motivos_vendas') }}
    )


    ,join_tables as (

        select
            venmot.id_pedido_venda
            ,mot.id_motivo_venda
            ,mot.nm_motivo_venda
            ,mot.cd_motivo_venda
            ,mot.dt_modificacao        

        from stg_vendas_pedidos_motivos venmot
        left join stg_motivos_vendas mot on mot.id_motivo_venda = venmot.id_motivo_venda
    )

    ,array_motivos as (
        select 
            id_pedido_venda
            ,case when upper(string_agg(mot.cd_motivo_venda)) like '%MARKETING%' then true else false end as is_motivo_marketing
            ,case when upper(string_agg(mot.cd_motivo_venda)) like '%PROMOTION%' then true else false end as is_motivo_promotion
            ,string_agg(mot.nm_motivo_venda, ' | ') as nm_motivo_venda
            ,string_agg(mot.cd_motivo_venda, ' | ') as cd_motivo_venda
        from join_tables mot 
        group by id_pedido_venda
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_pedido_venda']) }} as sk_motivo_venda
            ,array_motivos.*
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from array_motivos
    )


select * from refined

