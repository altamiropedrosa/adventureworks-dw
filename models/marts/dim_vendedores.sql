with 
    stg_vendedores as (
        select * 
        from {{ ref('stg_sap_adw_vendedores') }}
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


    ,vendas_vendedor as (
        select 
            id_vendedor
            ,min(dt_pedido) as dt_primeira_venda
            ,max(dt_pedido) as dt_ultima_venda
            ,count(distinct id_pedido_venda) as qt_total_venda
            ,sum(vl_total_item) as vl_total_venda
        from int_vendas_pedidos_itens
        group by id_vendedor
    )


    ,join_tables as (
        select 
            ven.id_vendedor
            ,pes.nm_pessoa as nm_vendedor
            ,pes.ds_email_primario as ds_email_primario_vendedor
            ,pes.ds_email_secundario as ds_email_secundario_vendedor
            ,pes.nm_tipos_telefones
            ,pes.nr_telefones
            --DADOS DE ENDEREÇO
            ,ent.id_tipo_endereco
            ,ent.nm_tipo_endereco
            ,ent.id_endereco
            ,ent.ds_endereco
            ,ent.nm_cidade
            ,ent.cd_estado
            ,ent.nm_estado
            ,ent.nr_cep
            ,ent.cd_pais
            ,ent.nm_pais
            ,ter.id_territorio
            ,ter.nm_territorio
            ,ter.ds_grupo_territorial
            --DADOS DE VENDAS DO VENDEDOR
            ,ven.vl_cota_vendas
            ,ven.vl_bonus
            ,ven.pc_comissao
            ,ven.vl_vendas_ytd --campo calculado?
            ,ven.vl_vendas_ultimo_ano --campo_calculado?
            ,tot.dt_primeira_venda
            ,tot.dt_ultima_venda
            ,coalesce(tot.qt_total_venda,0) as qt_total_venda
            ,round(coalesce(tot.vl_total_venda,0),2) as vl_total_venda 
            ,ven.dt_modificacao
        from stg_vendedores ven
        inner join int_pessoas pes on pes.id_pessoa = ven.id_vendedor
        inner join int_entidades ent on ent.id_entidade = ven.id_vendedor
        left join int_territorios ter on ter.id_territorio = ven.id_territorio
        left join vendas_vendedor tot on tot.id_vendedor = ven.id_vendedor
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_vendedor']) }} as sk_vendedor
            ,join_tables.*
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from join_tables
    )


select * from refined