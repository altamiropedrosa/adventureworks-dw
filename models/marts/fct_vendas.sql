with
    dim_datas as (
        select sk_data, date_day, month_of_year, year_number
        from {{ ref('dim_datas') }}
    )
    ,dim_clientes as (
        select sk_cliente, id_cliente, cd_tipo_cliente, nm_cliente, nm_cidade, nm_estado, nm_pais, nm_territorio, ds_grupo_territorial
        from {{ ref('dim_clientes') }}
    )
    ,dim_vendedores as (
        select sk_vendedor, id_vendedor, nm_vendedor
        from {{ ref('dim_vendedores') }}
    )
    ,dim_produtos as (
        select sk_produto, id_produto, nm_produto, nm_categoria_produto, nm_subcategoria_produto, 
               nm_modelo_produto, vl_tabela_produto
        from {{ ref('dim_produtos') }}
    )
    ,dim_formas_envio as (
        select sk_forma_envio, id_forma_envio, nm_forma_envio
        from {{ ref('dim_formas_envio') }}
    )
    ,dim_motivos_vendas as (
        select sk_motivo_venda, id_pedido_venda, cd_motivo_venda, nm_motivo_venda, is_motivo_venda_promotion
        from {{ ref('dim_motivos_vendas') }}
    )
    ,dim_cartoes_creditos as (
        select sk_cartao_credito, id_responsavel, nm_responsavel, id_cartao_credito, cd_tipo_cartao_credito
        from {{ ref('dim_cartoes_creditos') }}
    )
    ,int_vendas_pedidos_itens as (
        select *
        from {{ ref('int_vendas_pedidos_itens') }}
    )
    
    ,join_tables as (

        select 
		    --DADOS DO PEDIDO
            pedite.id_pedido_venda
            ,pedite.id_pedido_venda_item
            ,pedite.cd_status
            ,pedite.is_pedido_realizado_pelo_cliente
            ,dtped.sk_data as sk_data_pedido
            ,dtpag.sk_data as sk_data_pagamento
            ,dtenv.sk_data as sk_data_envio
            ,frm.sk_forma_envio
            ,motven.sk_motivo_venda
            ,car.sk_cartao_credito
            ,cli.sk_cliente
            ,ven.sk_vendedor
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
        left join dim_clientes cli on cli.id_cliente = pedite.id_cliente
        left join dim_vendedores ven on ven.id_vendedor = pedite.id_vendedor
        left join dim_produtos prd on prd.id_produto = pedite.id_produto
        left join dim_formas_envio frm on frm.id_forma_envio = pedite.id_forma_envio
        left join dim_motivos_vendas motven on motven.id_pedido_venda = pedite.id_pedido_venda
        left join dim_cartoes_creditos car on car.id_cartao_credito = pedite.id_cartao_credito

    )

    ,refined as (

        select 
            {{ dbt_utils.generate_surrogate_key(['id_pedido_venda','id_pedido_venda_item']) }} as sk_fct_vendas_pedidos
            ,join_tables.*
        from join_tables

    )


select * from refined

