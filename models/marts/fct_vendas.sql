with
    /*,stg_estados_impostos as (
        select id_imposto, id_estado, cd_tipo_imposto, pc_imposto
        from {{ ref('stg_sap_adw_estados_impostos') }}
    )*/
    dim_datas as (
        select sk_data, date_day, month_of_year, year_number
        from {{ ref('dim_datas') }}
    )
    ,dim_formas_envio as (
        select sk_forma_envio, id_forma_envio, nm_forma_envio
        from {{ ref('dim_formas_envio') }}
    )
    ,dim_produtos as (
        select sk_produto, id_produto, nm_produto, nm_categoria_produto, nm_subcategoria_produto, 
               nm_modelo_produto, vl_tabela_produto
        from {{ ref('dim_produtos') }}
    )
    ,dim_promocoes as (
        select sk_promocao, id_produto, id_promocao, nm_promocao, ds_tipo_promocao, ds_categoria_promocao
        from {{ ref('dim_promocoes') }}
    )
    ,dim_taxas_cambio as (
        select sk_taxa_cambio, id_taxa_cambio, nm_moeda_de_taxa_cambio, nm_moeda_para_taxa_cambio
        from {{ ref('dim_taxas_cambio') }}
    )
    ,dim_territorios as (
        select *
        from {{ ref('dim_territorios') }}
    )
    ,dim_vendedores as (
        select sk_vendedor, id_vendedor, nm_vendedor
        from {{ ref('dim_vendedores') }}
    )
    ,dim_cartoes_creditos as (
        select sk_cartao_credito, id_responsavel, nm_responsavel, id_cartao_credito, cd_tipo_cartao_credito
        from {{ ref('dim_cartoes_creditos') }}
    )
    ,dim_clientes as (
        select sk_cliente, id_cliente, cd_tipo_cliente, nm_cliente, nm_cidade, nm_estado, nm_pais
        from {{ ref('dim_clientes') }}
    )
    ,dim_enderecos as (
        select *
        from {{ ref('dim_enderecos') }}
    )
    ,dim_motivos_vendas as (
        select *
        from {{ ref('dim_motivos_vendas') }}
    )
    ,int_vendas_pedidos_itens as (
        select id_pedido_venda, id_pedido_venda_item, id_territorio, id_vendedor,
            id_cartao_credito, id_cliente, id_endereco_cobranca, id_endereco_entrega,
            id_forma_envio, id_produto, id_promocao, id_taxa_cambio,
            dt_pedido, dt_pagamento, dt_envio, cd_status, is_pedido_realizado_cliente, 
            qt_pedido_item, vl_unitario_item, vl_bruto_item, vl_desconto_item, 
            vl_liquido_item, vl_imposto_item, vl_frete_item, vl_total_item
        from {{ ref('int_vendas_pedidos_itens') }}
    )
    
    ,join_tables as (
        select 
            pedite.id_pedido_venda
            ,pedite.id_pedido_venda_item
            ,pedite.cd_status
            ,pedite.is_pedido_realizado_cliente
            --DADOS DE DATA DE PEDIDO
            ,dtped.sk_data as sk_data_pedido
            ,dtped.date_day as dt_pedido
            ,dtped.month_of_year as mes_pedido
            ,dtped.year_number as ano_pedido
            --DADOS DE DATA DE PEDIDO
            ,dtpag.sk_data as sk_data_pagamento
            ,dtpag.date_day as dt_pagamento
            ,dtpag.month_of_year as mes_pagamento
            ,dtpag.year_number as ano_pagamento
            --DADOS DE DATA DE ENVIO
            ,dtenv.sk_data as sk_data_envio
            ,dtenv.date_day as dt_envio
            ,dtenv.month_of_year as mes_envio
            ,dtenv.year_number as ano_envio
            --DADOS DO CLIENTE
            ,cli.sk_cliente
            ,cli.cd_tipo_cliente            
            ,cli.nm_cliente
            ,cli.nm_cidade as nm_cidade_cliente
            ,cli.nm_estado as nm_estado
            ,cli.nm_pais as nm_pais_cliente
            --DADOS DE TERRITORIO
            ,ter.sk_territorio
            ,ter.id_territorio
            ,ter.nm_territorio
            ,ter.cd_pais
            ,ter.nm_pais
            ,ter.cd_moeda
            ,ter.nm_moeda
            ,ter.ds_grupo_territorio            
            --DADOS DO VENDEDOR
            ,ven.sk_vendedor
            ,ven.nm_vendedor
            --DADOS DE ENTREGA
            ,endcob.sk_endereco as sk_endereco_cobranca
            ,endent.sk_endereco as sk_endereco_entrega
		    --DADOS DE MOTIVOS DE VENDAS
            ,motven.is_motivo_promotion
            ,motven.cd_motivo_venda
            ,motven.nm_motivo_venda
            --DADOS DE FORMAS ENVIO
            ,frm.sk_forma_envio
            ,frm.nm_forma_envio
            --DADOS DO CARTÃO DE CRÉDITO
            ,car.sk_cartao_credito
            ,car.cd_tipo_cartao_credito
            ,car.nm_responsavel as nm_responsavel_cartao_credito
            --DADOS DE TAXA DE CAMBIO
            ,cam.sk_taxa_cambio
            ,cam.nm_moeda_de_taxa_cambio
            ,cam.nm_moeda_para_taxa_cambio
            --DADOS DE PROMOÇÃO
            ,prm.sk_promocao
            ,prm.nm_promocao
            ,prm.ds_tipo_promocao
            ,prm.ds_categoria_promocao
            --DADOS DE PRODUTOS
            ,prd.sk_produto
            ,prd.nm_produto
            ,prd.nm_categoria_produto
            ,prd.nm_subcategoria_produto
            ,prd.nm_modelo_produto
            ,prd.vl_tabela_produto
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
        left join dim_cartoes_creditos car on car.id_cartao_credito = pedite.id_cartao_credito
        left join dim_clientes cli on cli.id_cliente = pedite.id_cliente
        left join dim_enderecos endcob on endcob.id_endereco = pedite.id_endereco_cobranca
        left join dim_enderecos endent on endent.id_endereco = pedite.id_endereco_entrega
        left join dim_formas_envio frm on frm.id_forma_envio = pedite.id_forma_envio
        left join dim_produtos prd on prd.id_produto = pedite.id_produto
        left join dim_promocoes prm on prm.id_promocao = pedite.id_promocao and pedite.id_produto = prm.id_produto
        left join dim_taxas_cambio cam on cam.id_taxa_cambio = pedite.id_taxa_cambio
        left join dim_territorios ter on ter.id_territorio = pedite.id_territorio
        left join dim_vendedores ven on ven.id_vendedor = pedite.id_vendedor
        left join dim_motivos_vendas motven on motven.id_pedido_venda = pedite.id_pedido_venda

    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_pedido_venda','id_pedido_venda_item']) }} as sk_fct_vendas_pedidos
            ,join_tables.*
        from join_tables
    )


select * from refined


/*select sum(vl_bruto_item) as vl_bruto 
    ,sum(vl_desconto_item) as vl_desconto
    ,sum(vl_liquido_item) as vl_liquido
    ,sum(vl_imposto_item) as vl_imposto
    ,sum(vl_frete_item) as vl_frete
    ,sum(vl_total_item) as vl_total
from join_tables
where ano_pedido = 2011*/

