with 
    top_5_cidades as (
        select 
            nm_cidade
            ,round(sum(vl_total_item),2) as vl_total_pedido
        from {{ ref('fct_vendas_pedidos') }}
        where nm_cidade is not null
        group by nm_cidade
        order by vl_total_pedido desc
        limit 5
    )

    ,top_5_cidades_vendas as (
        select 
            fct.nm_cliente
            ,fct.nm_produto
            ,fct.cd_tipo_cartao_credito
            ,fct.cd_motivo_venda
            ,fct.dt_pedido 
            ,fct.cd_status
            ,fct.nm_cidade
            ,fct.nm_estado
            ,fct.nm_pais
            ,count(distinct fct.id_pedido_venda) as total_pedido
            ,sum(fct.qt_pedido_item) as qt_itens_pedido
            ,round(sum(fct.vl_total_item),2) as vl_total_pedido
        from {{ ref('fct_vendas_pedidos') }} fct
        inner join top_5_cidades top5 on top5.nm_cidade = fct.nm_cidade
        group by 
            fct.nm_cliente
            ,fct.nm_produto
            ,fct.cd_tipo_cartao_credito
            ,fct.cd_motivo_venda
            ,fct.dt_pedido 
            ,fct.cd_status
            ,fct.nm_cidade
            ,fct.nm_estado
            ,fct.nm_pais
    )

select * from top_5_cidades_vendas