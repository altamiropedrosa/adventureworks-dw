with 
    total_vendas_mes_ano as (
        select 
            mes_pedido
            ,ano_pedido 
            ,count(distinct id_pedido_venda) as total_pedido
            ,sum(qt_pedido_item) as qt_itens_pedido
            ,round(sum(vl_total_item),2) as vl_total_pedido
        from {{ ref('fct_vendas_pedidos') }}
        group by mes_pedido, ano_pedido 
        order by 1, 2
    )

select * from total_vendas_mes_ano