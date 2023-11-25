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
    ,int_vendas_pedidos_itens as (
        select * 
        from {{ ref('int_vendas_pedidos_itens') }}
    )

    ,vendas_vendedor as (

        select 
            id_vendedor
            ,min(dt_pedido) as dt_primeira_venda
            ,max(dt_pedido) as dt_ultima_venda
            ,count(distinct id_pedido_venda) as qt_pedidos_venda
            ,sum(vl_total_item) as vl_total_venda
        from int_vendas_pedidos_itens
        group by id_vendedor

    )

    ,join_tables as (

        select 
            ven.id_vendedor
            ,pes.nm_pessoa as nm_vendedor
            ,pes.ds_email_primario
            ,pes.ds_email_secundario
            ,pes.nm_tipos_telefones
            ,pes.nr_telefones
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
            --DADOS DE VENDAS DO VENDEDOR
            ,round(ven.vl_cota_vendas,2) as vl_cota_vendas
            ,round(ven.vl_bonus,2) as vl_bonus
            ,round(ven.pc_comissao,1) as pc_comissao
            ,tot.dt_primeira_venda
            ,tot.dt_ultima_venda
            ,coalesce(tot.qt_pedidos_venda,0) as qt_pedidos_venda
            ,round(coalesce(tot.vl_total_venda,0),2) as vl_total_venda 
            ,ven.dt_modificacao
        from stg_vendedores ven
        inner join int_pessoas pes on pes.id_pessoa = ven.id_vendedor
        inner join int_entidades ent on ent.id_entidade = ven.id_vendedor
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