with
    stg_pedidos_vendas as (
        select *
        from {{ ref('stg_sap_adw_pedidos_vendas') }}
    )
    ,stg_pedidos_vendas_itens as (
        select *
        from {{ ref('stg_sap_adw_pedidos_vendas_itens') }}
    )

    ,join_tables as (
        select
            ped.id_pedido_venda
            ,ped.nr_revisao
            ,ped.dt_pedido
            ,ped.dt_pagamento
            ,ped.dt_envio
            ,ped.cd_status
            ,ped.in_pedido_online
            ,ped.nr_ordem_compra
            ,ped.nr_conta_financeira
            ,ped.id_cliente
            ,ped.id_vendedor
            ,ped.id_territorio
            ,ped.id_endereco_cobranca
            ,ped.id_endereco_entrega
            ,ped.id_forma_envio
            ,ped.id_cartao_credito
            ,ped.cd_aprovacao_cartao_credito
            ,ped.id_taxa_cambio
            ,ped.vl_subtotal
            ,ped.vl_imposto
            ,ped.vl_frete
            ,ped.vl_pago
            ,ped.ds_comentario

            ,pedite.id_pedido_venda_item
            ,pedite.nr_rastreamento
            ,pedite.qt_pedido
            ,pedite.id_produto
            ,pedite.id_promocao
            ,pedite.vl_unitario
            ,pedite.vl_desconto
        from stg_pedidos_vendas ped
        left join stg_pedidos_vendas_itens pedite on pedite.id_pedido_venda = ped.id_pedido_venda
    )
    
    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_pedido_venda','id_pedido_venda_item']) }} as sk_pedido_venda_item
            ,join_tables.*
        from join_tables
    )

select * from refined where id_pedido_venda = 43664
 