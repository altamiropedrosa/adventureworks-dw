with 
    stg_clientes as (
        select id_cliente, id_pessoa, id_loja, id_territorio, dt_modificacao
        from {{ ref('stg_sap_adw_clientes') }}
    )
    ,stg_lojas as (
        select id_loja, nm_loja
        from {{ ref('stg_sap_adw_lojas') }}
    )
    ,stg_vendas_territorios as (
        select id_territorio, nm_territorio, cd_pais, ds_grupo_territorial
        from {{ ref('stg_sap_adw_vendas_territorios') }}
    )
    ,int_entidades as (
        select * 
        from {{ ref('int_entidades_related') }}
    )
    ,int_pessoas as (
        select * 
        from {{ ref('int_pessoas_related') }}
    )
    ,int_territorios as (
        select * 
        from {{ ref('int_territorios_related') }}
    )
    ,int_vendas_pedidos_itens as (
        select * 
        from {{ ref('int_vendas_pedidos_itens') }}
    )


    ,ltv_cliente as (
        select 
            id_cliente
            ,min(dt_pedido) as dt_primeiro_pedido
            ,max(dt_pedido) as dt_ultimo_pedido
            ,count(distinct id_pedido_venda) as qt_pedido
            ,count(distinct id_pedido_venda) > 1 as is_cliente_recorrente
            ,DATETIME_DIFF(cast(max(dt_pedido) as datetime), cast(min(dt_pedido) as datetime), MONTH) as qt_meses_priult_pedido
            ,sum(vl_bruto_item) as vl_bruto_pedido
            ,sum(vl_total_item) as vl_total_pedido
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
            ,pes.ds_email_primario as ds_email_primario_cliente
            ,pes.ds_email_secundario as ds_email_secundario_cliente
            ,pes.nm_tipos_telefones as nm_tipos_telefones_cliente
            ,pes.nr_telefones as nr_telefones_cliente
            ,cli.id_territorio
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
            ,ent.ds_email_primario_contato as ds_email_primario_cliente
            ,ent.ds_email_secundario_contato as ds_email_secundario_cliente
            ,ent.nm_tipos_telefones_contato as nm_tipos_telefones_cliente
            ,ent.nr_telefones_contato as nr_telefones_cliente
            ,cli.id_territorio
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
            ,cli.ds_email_primario_cliente
            ,cli.ds_email_secundario_cliente
            ,cli.nm_tipos_telefones_cliente
            ,cli.nr_telefones_cliente
            --DADOS DE ENDEREÇO
            ,ent.id_tipo_endereco as id_tipo_endereco_cliente
            ,ent.nm_tipo_endereco as nm_tipo_endereco_cliente
            ,ent.id_endereco  as id_endereco_cliente
            ,ent.ds_endereco as ds_endereco_cliente
            ,ent.nm_cidade as nm_cidade_cliente
            ,ent.cd_estado as cd_estado_cliente
            ,ent.nm_estado as nm_estado_cliente
            ,ent.nr_cep as nr_cep_cliente
            ,ent.cd_pais as cd_pais_cliente
            ,ent.nm_pais as nm_pais_cliente
            ,ter.id_territorio as id_territorio_cliente
            ,ter.nm_territorio as nm_territorio_cliente
            ,ter.ds_grupo_territorial as ds_grupo_territorial_cliente
            ,ter.cd_moeda as cd_moeda_territorio_cliente
            ,ter.nm_moeda as nm_moeda_territorio_cliente
            --DADOS DE LIFE TIME VALUE
            ,coalesce(ltv.is_cliente_recorrente,false) as is_cliente_recorrente
            ,ltv.dt_primeiro_pedido
            ,ltv.dt_ultimo_pedido
            ,coalesce(ltv.qt_pedido,0) as qt_pedido
            ,coalesce(ltv.vl_total_pedido,0) as vl_total_pedido
            ,cli.dt_modificacao
        from union_clientes cli
        left join int_entidades ent on ent.id_entidade = cli.id_entidade
        left join int_territorios ter on ter.id_territorio = cli.id_territorio
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

