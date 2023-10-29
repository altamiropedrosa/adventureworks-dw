with
    dim_datas as (
        select *
        from {{ ref('dim_datas') }}
    )
    ,dim_cartoes_credito as (
        select *
        from {{ ref('dim_cartoes_credito') }}
    )
    ,dim_clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )
    ,dim_enderecos as (
        select *
        from {{ ref('dim_enderecos') }}
    )
    ,dim_formas_envio as (
        select *
        from {{ ref('dim_formas_envio') }}
    )
    ,dim_produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )
    ,dim_promocoes as (
        select *
        from {{ ref('dim_promocoes') }}
    )
    ,dim_motivos_vendas as (
        select *
        from {{ ref('dim_motivos_vendas') }}
    )
    ,dim_taxas_cambio as (
        select *
        from {{ ref('dim_taxas_cambio') }}
    )
    ,dim_territorios as (
        select *
        from {{ ref('dim_territorios') }}
    )
    ,dim_vendedores as (
        select *
        from {{ ref('dim_vendedores') }}
    )
    ,stg_vendas_pedidos_motivos as (
        select *
        from {{ ref('stg_sap_adw_vendas_pedidos_motivos') }}
    )
    ,int_vendas_pedidos_itens as (
        select *
        from {{ ref('int_vendas_pedidos_itens') }}
    )
    
    ,join_tables as (
        select 
            pedite.id_pedido_venda
            ,pedite.id_pedido_venda_item
            ,pedite.cd_status
            ,pedite.is_pedido_online
            ,dtped.sk_data as sk_data_pedido
            ,dtpag.sk_data as sk_data_pagamento
            ,dtenv.sk_data as sk_data_envio
            ,cli.sk_cliente
            ,ven.sk_vendedor
            ,ter.sk_territorio
            ,endcob.sk_endereco as sk_endereco_cobranca
            ,endent.sk_endereco as sk_endereco_entrega
            ,frm.sk_forma_envio
            ,car.sk_cartao_credito
            ,cam.sk_taxa_cambio
            ,prm.sk_promocao
            ,prd.sk_produto
            /*METRICAS*/
            ,pedite.qt_pedido_item
            ,pedite.vl_unitario_item
            ,pedite.vl_bruto_item
            ,pedite.vl_desconto_item
            ,pedite.vl_liquido_item
            ,pedite.vl_imposto_item
            ,pedite.vl_frete_item
            ,pedite.vl_total_item
        from int_vendas_pedidos_itens as pedite
        left join dim_datas dtped on dtped.date_day = cast(pedite.dt_pedido as date)
        left join dim_datas dtpag on dtpag.date_day = cast(pedite.dt_pagamento as date)
        left join dim_datas dtenv on dtenv.date_day = cast(pedite.dt_envio as date)
        left join dim_cartoes_credito car on car.id_cartao_credito = pedite.id_cartao_credito
        left join dim_clientes cli on cli.id_cliente = pedite.id_cliente
        left join dim_enderecos endcob on endcob.id_endereco = pedite.id_endereco_cobranca
        left join dim_enderecos endent on endent.id_endereco = pedite.id_endereco_entrega
        left join dim_formas_envio frm on frm.id_forma_envio = pedite.id_forma_envio
        left join dim_produtos prd on prd.id_produto = pedite.id_produto
        left join dim_promocoes prm on prm.id_promocao = pedite.id_promocao
        left join stg_vendas_pedidos_motivos motven on motven.id_pedido_venda = pedite.id_pedido_venda
        left join dim_motivos_vendas mot on mot.id_motivo_venda = motven.id_motivo_venda
        left join dim_taxas_cambio cam on cam.id_taxa_cambio = pedite.id_taxa_cambio
        left join dim_territorios ter on ter.id_territorio = pedite.id_territorio
        left join dim_vendedores ven on ven.id_vendedor = pedite.id_vendedor
    )


--select * from join_tables

select id_pedido_venda
    ,sum(vl_bruto_item) as vl_bruto_item 
    ,sum(vl_desconto_item) as vl_desconto_item
    ,sum(vl_liquido_item) as vl_liquido_item
    ,sum(vl_imposto_item) as vl_imposto_item
    ,sum(vl_frete_item) as vl_frete_item
    ,sum(vl_total_item) as vl_total_item
from join_tables
group by id_pedido_venda
