with 
    stg_clientes as (
        select * from {{ ref('stg_sap_adw_clientes') }}
    )
    /*,int_pessoas as (
        select * from {{ ref('int_pessoas_joins') }}
    )*/
    ,stg_lojas as (
        select * from {{ ref('stg_sap_adw_lojas') }}
    )
    ,int_vendas_pedidos_itens as (
        select * from {{ ref('int_vendas_pedidos_itens') }}
    )
    ,stg_estados_impostos as (
        select id_imposto, id_estado, cd_tipo_imposto, pc_imposto
        from {{ ref('stg_sap_adw_estados_impostos') }}
    )

    ,stg_ltv as (
        select 
            id_cliente
            ,min(dt_pedido) as dt_primeiro_pedido_online
            ,count(distinct id_pedido_venda) as qt_pedidos_online
            ,DATETIME_DIFF(current_date(), cast(max(dt_pedido) as datetime), MONTH) as qt_meses_ultimo_pedido_online
            ,sum(vl_total_item) as vl_ltv_online
        from int_vendas_pedidos_itens
        group by id_cliente
    )

    ,join_tables as (
        select 
            cli.id_cliente
            ,case when cli.id_pessoa is not null then true else false end as is_cliente_online
            ,cli.id_pessoa
            /*,pescli.cd_tipo_pessoa
            ,coalesce(pescli.nm_pessoa,'Cliente Avulso') as nm_cliente
            ,pescli.cd_email_promocional
            ,pescli.ds_email
            ,pescli.nm_tipos_telefones
            ,pescli.nr_telefones
            ,pescli.nm_tipo_endereco
            ,pescli.id_endereco
            ,pescli.ds_endereco
            ,pescli.nm_cidade
            ,pescli.id_estado
            ,pescli.cd_estado
            ,pescli.nm_estado
            ,pescli.nr_cep
            ,pescli.cd_pais
            ,pescli.nm_pais
            ,pescli.ds_grupo_territorio*/
            ,cli.id_loja
            ,loj.nm_loja
            ,loj.id_vendedor
            --,pesven.nm_pessoa as nm_vendedor
            ,ltv.dt_primeiro_pedido_online
            ,coalesce(ltv.qt_pedidos_online,0) as qt_pedidos_online
            ,coalesce(ltv.qt_meses_ultimo_pedido_online,0) as qt_meses_ultimo_pedido_online
            ,coalesce(ltv.vl_ltv_online,0) as vl_ltv_online

        from stg_clientes cli
        --left join int_pessoas pescli on pescli.id_pessoa = cli.id_pessoa
        left join stg_lojas loj on loj.id_loja = cli.id_loja
        --left join int_pessoas pesven on pesven.id_pessoa = loj.id_vendedor
        left join stg_ltv ltv on ltv.id_cliente = cli.id_cliente
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_cliente']) }} as sk_cliente
            ,join_tables.*
        from join_tables
    )

select * from refined