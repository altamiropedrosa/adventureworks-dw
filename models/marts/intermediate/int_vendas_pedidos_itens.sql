with
    stg_vendas_pedidos as (
        select *
        from {{ ref('stg_sap_adw_vendas_pedidos') }}
    )
    ,stg_vendas_pedidos_itens as (
        select *
        from {{ ref('stg_sap_adw_vendas_pedidos_itens') }}
    )
    ,stg_promocoes_produtos as (
        select * from {{ ref('stg_sap_adw_promocoes_produtos') }}
    )
    ,stg_promocoes as (
        select * from {{ ref('stg_sap_adw_promocoes') }}
    )
    ,stg_vendas_impostos as (
        select * from {{ ref('stg_sap_adw_vendas_impostos') }}
    )
    ,stg_formas_envios as (
        select * from {{ ref('stg_sap_adw_formas_envios') }}
    )
    ,int_enderecos as (
        select * from {{ ref('int_enderecos_joins') }}
    )

    ,vendas_pedidos_itens_summary as (
        select  
            pedite.id_pedido_venda
            ,cast(count(id_pedido_venda_item) as int) as qt_itens_pedido
            ,cast((ped.vl_frete / count(id_pedido_venda_item)) as BIGNUMERIC) as vl_frete_ponderado
            ,cast((ped.vl_imposto / count(id_pedido_venda_item)) as BIGNUMERIC) as vl_imposto_ponderado
        from stg_vendas_pedidos_itens pedite 
        left join stg_vendas_pedidos ped on ped.id_pedido_venda = pedite.id_pedido_venda
        group by pedite.id_pedido_venda, ped.vl_frete, ped.vl_imposto
    )

    ,join_tables as (
        select
            ped.id_pedido_venda
            ,ped.nr_ordem_compra
            ,ped.nr_conta_financeira
            ,ped.nr_revisao_pedido
            ,ped.dt_pedido
            ,ped.dt_pagamento
            ,ped.dt_envio
            ,ped.cd_status
            ,ped.is_pedido_online
            ,ped.id_cliente
            ,ped.id_vendedor
            ,ped.id_territorio
            ,ped.id_endereco_cobranca
            ,ped.id_endereco_entrega
            ,ped.id_forma_envio
            ,ped.id_cartao_credito
            ,ped.cd_aprovacao_cartao_credito
            ,ped.id_taxa_cambio
            /*,round(ped.vl_subtotal,2) as vl_subtotal
            ,round(ped.vl_imposto,2) as vl_imposto
            ,round(ped.vl_frete,2) as vl_frete
            ,round(ped.vl_total,2) as vl_total*/

            ,pedite.id_pedido_venda_item
            ,pedite.nr_rastreamento
            ,pro.id_promocao
            ,pedite.id_produto
            ,pedite.qt_pedido_item
            ,pedite.vl_unitario_item
            ,round((pedite.vl_unitario_item * pedite.qt_pedido_item),2) as vl_bruto_item
            ,round((pedite.pc_desconto_item * pedite.vl_unitario_item * pedite.qt_pedido_item),2) as vl_desconto_item
            ,round(((1 - pedite.pc_desconto_item) * pedite.vl_unitario_item * pedite.qt_pedido_item),2) as vl_liquido_item
            ,round(som.vl_frete_ponderado,2) as vl_frete_item
            ,round(som.vl_imposto_ponderado,2) as vl_imposto_item
            ,round( ((1 - pedite.pc_desconto_item) * pedite.vl_unitario_item * pedite.qt_pedido_item)
                   +(som.vl_frete_ponderado + som.vl_imposto_ponderado),2) as vl_total_item
        from stg_vendas_pedidos ped
        left join stg_vendas_pedidos_itens pedite on pedite.id_pedido_venda = ped.id_pedido_venda
        left join stg_promocoes_produtos proprd on proprd.id_produto = pedite.id_produto 
                                               and proprd.id_promocao = pedite.id_promocao
        left join stg_promocoes pro on pro.id_promocao = proprd.id_promocao 
        left join vendas_pedidos_itens_summary som on som.id_pedido_venda = ped.id_pedido_venda
    )
    

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_pedido_venda','id_pedido_venda_item']) }} as sk_pedido_venda_item
            ,join_tables.*
        from join_tables
    )

select * from refined where id_pedido_venda = 46607

/*
select distinct id_pedido_venda, 
    sum(vl_bruto_item) as vl_bruto_item, 
    sum(vl_desconto_item) as vl_desconto_item,
    sum(vl_liquido_item) as vl_liquido_item,
    sum(vl_imposto_item) as vl_imposto_item, 
    sum(vl_frete_item) as vl_frete_item, 
    sum(vl_total_item) as vl_total_item
from join_tables
group by id_pedido_venda
*/






 