with
    stg_vendas_pedidos as (
        select *
        from {{ ref('stg_sap_adw_vendas_pedidos') }}
    )
    ,stg_vendas_pedidos_itens as (
        select *
        from {{ ref('stg_sap_adw_vendas_pedidos_itens') }}
    )
    ,stg_clientes as (
        select *
        from {{ ref('stg_sap_adw_clientes') }}
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
            --DADOS DO PEDIDO DE VENDAS
            ped.id_pedido_venda
            ,ped.nr_ordem_compra
            ,ped.nr_conta_financeira
            ,ped.nr_revisao_pedido
            ,ped.cd_aprovacao_cartao_credito
            ,ped.cd_status
            ,ped.is_pedido_realizado_cliente
            --DADOS DE DATAS
            ,ped.dt_pedido
            ,ped.dt_pagamento
            ,ped.dt_envio
            --DADOS FK
            ,cli.id_loja
            ,ped.id_cliente
            ,ped.id_vendedor
            ,ped.id_territorio
            ,ped.id_endereco_cobranca
            ,ped.id_endereco_entrega
            ,ped.id_forma_envio
            ,ped.id_cartao_credito
            ,ped.id_taxa_cambio
            --DADOS DOS ITENS DO PEDIDO DE VENDAS
            ,pedite.id_pedido_venda_item
            ,pedite.nr_rastreamento
            ,pedite.id_promocao
            ,pedite.id_produto
            ,pedite.qt_pedido_item
            ,pedite.vl_unitario_item
            ,(pedite.vl_unitario_item * pedite.qt_pedido_item) as vl_bruto_item
            ,(pedite.pc_desconto_item * pedite.vl_unitario_item * pedite.qt_pedido_item) as vl_desconto_item
            ,((1 - pedite.pc_desconto_item) * pedite.vl_unitario_item * pedite.qt_pedido_item) as vl_liquido_item
            ,som.vl_frete_ponderado as vl_frete_item
            ,som.vl_imposto_ponderado as vl_imposto_item
            ,((1 - pedite.pc_desconto_item) * pedite.vl_unitario_item * pedite.qt_pedido_item)
                   +(som.vl_frete_ponderado + som.vl_imposto_ponderado) as vl_total_item
        from stg_vendas_pedidos_itens pedite
        left join stg_vendas_pedidos ped on ped.id_pedido_venda = pedite.id_pedido_venda
        left join vendas_pedidos_itens_summary som on som.id_pedido_venda = ped.id_pedido_venda
        left join stg_clientes cli on cli.id_cliente = ped.id_cliente
    )
    

select * from join_tables --where id_pedido_venda = 48320

/*
select --distinct id_pedido_venda, 
    sum(vl_bruto_item) as vl_bruto_item, 
    sum(vl_desconto_item) as vl_desconto_item,
    sum(vl_liquido_item) as vl_liquido_item,
    sum(vl_imposto_item) as vl_imposto_item, 
    sum(vl_frete_item) as vl_frete_item, 
    sum(vl_total_item) as vl_total_item
from join_tables
where extract(year from cast(dt_pedido as date)) = 2011
--group by id_pedido_venda
*/








 