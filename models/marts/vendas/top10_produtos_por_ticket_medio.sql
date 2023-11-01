with 
    top_10_produtos as (
        select 
            nm_produto
            ,round(sum(vl_liquido_item)/count(distinct id_pedido_venda),2) as vl_ticket_medio
        from {{ ref('fct_vendas_pedidos') }}
        group by 
            nm_produto
        order by vl_ticket_medio desc
        limit 10
    )

    ,top_10_produtos_vendas as (
        select 
            fct.nm_produto
            ,fct.mes_pedido
            ,fct.ano_pedido
            ,fct.nm_cidade
            ,fct.nm_estado
            ,fct.nm_pais
            ,round(sum(fct.vl_liquido_item)/count(distinct fct.id_pedido_venda),2) as vl_ticket_medio
        from {{ ref('fct_vendas_pedidos') }} fct
        inner join top_10_produtos top10 on top10.nm_produto = fct.nm_produto
        group by 
            nm_produto
            ,mes_pedido
            ,ano_pedido
            ,nm_cidade
            ,nm_estado
            ,nm_pais
        order by 1,2,3
    )


select * from top_10_produtos_vendas