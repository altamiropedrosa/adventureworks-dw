with 
    top_produto_promocao as (
        select 
            nm_produto
            ,cd_motivo_venda
            ,sum(qt_pedido_item) as qt_itens_pedido
        from {{ ref('fct_vendas_pedidos') }}
        where cd_motivo_venda = 'Promotion'
        group by nm_produto, cd_motivo_venda
        order by qt_itens_pedido desc
        limit 10
    )

select * from top_produto_promocao