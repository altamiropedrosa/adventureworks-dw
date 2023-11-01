with 
    total_vendas as (
        select 
            nm_produto
            ,nm_cliente
            ,cd_motivo_venda
            ,cd_tipo_cartao_credito
            ,cd_status
            ,nm_cidade
            ,nm_estado
            ,nm_pais
            ,dt_pedido  
            ,count(distinct id_pedido_venda) as total_pedido
            ,sum(qt_pedido_item) as qt_itens_pedido
            ,round(sum(vl_total_item),2) as vl_total_pedido
        from {{ ref('fct_vendas_pedidos') }}
        group by nm_produto, nm_cliente, cd_motivo_venda, cd_tipo_cartao_credito,
            cd_status, nm_cidade, nm_estado, nm_pais, dt_pedido
    )

select * from total_vendas