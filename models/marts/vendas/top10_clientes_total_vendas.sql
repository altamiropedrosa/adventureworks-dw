with 
    top_10_cliente as (
        select 
            sk_cliente
            ,round(sum(vl_total_item),2) as vl_total_pedido
        from {{ ref('fct_vendas_pedidos') }}
        group by sk_cliente
        order by vl_total_pedido desc
        limit 10
    )

    ,top_10_clientes_vendas as (
        select 
            nm_cliente
            ,nm_produto
            ,cd_tipo_cartao_credito
            ,cd_motivo_venda
            ,dt_pedido 
            ,cd_status
            ,nm_cidade
            ,nm_estado
            ,nm_pais
            ,count(distinct id_pedido_venda) as total_pedido
            ,sum(qt_pedido_item) as qt_itens_pedido
            ,round(sum(vl_total_item),2) as vl_total_pedido
        from {{ ref('fct_vendas_pedidos') }} fct
        inner join top_10_cliente top10 on top10.sk_cliente = fct.sk_cliente
        group by 
            nm_cliente
            ,nm_produto
            ,cd_tipo_cartao_credito
            ,cd_motivo_venda
            ,dt_pedido 
            ,cd_status
            ,nm_cidade
            ,nm_estado
            ,nm_pais
    )

select * from top_10_clientes_vendas