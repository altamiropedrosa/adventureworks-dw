with
    dim_cartoes_credito as (
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
    ,stg_pedidos_vendas_motivos as (
        select *
        from {{ ref('stg_sap_adw_pedidos_vendas_motivos') }}
    )
    ,pedido_vendas_itens as (
        select *
        from {{ ref('int_pedido_vendas_itens') }}
    )
    
    ,join_tables as (
        select 
            pedite.id_pedido_venda
            ,pedite.nr_revisao
            ,pedite.dt_pedido
            ,pedite.dt_pagamento
            ,pedite.dt_envio
            ,pedite.cd_status
            ,pedite.in_pedido_online
            ,pedite.nr_ordem_compra
            ,pedite.nr_conta_financeira
            ,cli.sk_cliente
            ,ven.sk_vendedor
            ,ter.sk_territorio
            ,endcob.sk_endereco as sk_endereco_cobranca
            ,endent.sk_endereco as sk_endereco_entrega
            ,frm.sk_forma_envio
            ,car.sk_cartao_credito
            ,pedite.cd_aprovacao_cartao_credito
            ,cam.sk_taxa_cambio
            ,pedite.vl_subtotal
            ,pedite.vl_imposto
            ,pedite.vl_frete
            ,pedite.vl_pago

            ,pedite.id_pedido_venda_item
            ,pedite.nr_rastreamento
            ,pedite.qt_pedido
            ,pro.sk_produto
            ,prm.sk_promocao
            ,pedite.vl_unitario
            ,pedite.vl_desconto
        from pedido_vendas_itens as pedite
        left join dim_cartoes_credito car on car.id_cartao_credito = pedite.id_cartao_credito
        left join dim_clientes cli on cli.id_cliente = pedite.id_cliente
        left join dim_enderecos endcob on endcob.id_endereco = pedite.id_endereco_cobranca
        left join dim_enderecos endent on endent.id_endereco = pedite.id_endereco_entrega
        left join dim_formas_envio frm on frm.id_forma_envio = pedite.id_forma_envio
        left join dim_produtos pro on pro.id_produto = pedite.id_produto
        left join dim_promocoes prm on prm.id_promocao = pedite.id_promocao
        left join stg_pedidos_vendas_motivos motven on motven.id_pedido_venda = pedite.id_pedido_venda
        left join dim_motivos_vendas mot on mot.id_motivo_venda = motven.id_motivo_venda
        left join dim_taxas_cambio cam on cam.id_taxa_cambio = pedite.id_taxa_cambio
        left join dim_territorios ter on ter.id_territorio = pedite.id_territorio
        left join dim_vendedores ven on ven.id_vendedor = pedite.id_vendedor
    )

select * from join_tables    