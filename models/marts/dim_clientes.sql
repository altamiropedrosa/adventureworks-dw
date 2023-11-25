with 
    stg_clientes as (
        select id_cliente, id_pessoa, id_loja, id_territorio, dt_modificacao
        from {{ ref('stg_sap_adw_clientes') }}
    )
    ,stg_lojas as (
        select id_loja, nm_loja
        from {{ ref('stg_sap_adw_lojas') }}
    )
    ,int_entidades as (
        select * 
        from {{ ref('int_entidades_related') }}
    )
    ,int_pessoas as (
        select * 
        from {{ ref('int_pessoas_related') }}
    )
    ,int_vendas_pedidos_itens as (
        select * 
        from {{ ref('int_vendas_pedidos_itens') }}
    )

    ,ltv_cliente as (
        select 
            id_cliente
            ,min(dt_pedido) as dt_primeira_compra
            ,max(dt_pedido) as dt_ultima_compra
            ,count(distinct id_pedido_venda) as qt_pedidos_compra
            ,count(distinct id_pedido_venda) > 1 as is_cliente_recorrente
            ,DATETIME_DIFF(cast(max(dt_pedido) as datetime), cast(min(dt_pedido) as datetime), MONTH) as qt_meses_priult_compra
            ,sum(vl_bruto_item) as vl_bruto_compra
            ,sum(vl_total_item) as vl_total_compra
        from int_vendas_pedidos_itens
        group by id_cliente
    )


    ,cliente_varejo as (
        select 
            --DADOS DO CLIENTE
            cli.id_cliente
            ,cli.id_pessoa as id_entidade
            ,'Varejo' as cd_tipo_cliente
            ,pes.nm_pessoa as nm_cliente
            ,pes.ds_email_primario
            ,pes.ds_email_secundario
            ,pes.nm_tipos_telefones
            ,pes.nr_telefones
            ,cli.dt_modificacao
        from stg_clientes cli
        inner join int_pessoas pes on pes.id_pessoa = cli.id_pessoa
        where cli.id_loja is null and cli.id_pessoa is not null
    )
    ,cliente_atacado as (
        select 
            --DADOS DO CLIENTE
            cli.id_cliente
            ,loj.id_loja as id_entidade
            ,'Atacado' as cd_tipo_cliente
            ,loj.nm_loja as nm_cliente
            ,ent.ds_email_primario
            ,ent.ds_email_secundario
            ,ent.nm_tipos_telefones
            ,ent.nr_telefones
            ,cli.dt_modificacao
        from stg_clientes cli
        inner join stg_lojas loj on loj.id_loja = cli.id_loja
        inner join int_entidades ent on ent.id_entidade = loj.id_loja
        left join int_pessoas pes on pes.id_pessoa = cli.id_pessoa
        where cli.id_loja is not null
    )

    ,union_clientes as (
        select * from cliente_varejo
        union all
        select * from cliente_atacado
    )

    ,join_tables as (
        select 
            --DADOS DO CLIENTE
            cli.id_cliente
            ,cli.id_entidade
            ,cli.cd_tipo_cliente
            ,cli.nm_cliente
            ,cli.ds_email_primario
            ,cli.ds_email_secundario
            ,cli.nm_tipos_telefones
            ,cli.nr_telefones
            --DADOS DE ENDEREÇO
            ,ent.id_endereco
            ,ent.ds_endereco
            ,ent.nm_cidade
            ,ent.cd_estado
            ,ent.nm_estado
            ,ent.nr_cep
            ,ent.cd_pais
            ,ent.nm_pais
            ,ent.nm_territorio
            ,ent.ds_grupo_territorial
            --DADOS DE LIFE TIME VALUE
            ,coalesce(ltv.is_cliente_recorrente,false) as is_cliente_recorrente
            ,ltv.dt_primeira_compra
            ,ltv.dt_ultima_compra
            ,coalesce(ltv.qt_pedidos_compra,0) as qt_pedidos_compra
            ,round(coalesce(ltv.vl_total_compra,0),2) as vl_total_compra
            ,cli.dt_modificacao
        from union_clientes cli
        left join int_entidades ent on ent.id_entidade = cli.id_entidade
        left join ltv_cliente ltv on ltv.id_cliente = cli.id_cliente
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_cliente']) }} as sk_cliente
            ,join_tables.*
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from join_tables
    )


select * from refined

