with
    stg_entidades as (
        select *
        from {{ ref('stg_sap_adw_entidades') }}
    )
    ,stg_enderecos as (
        select * 
        from {{ ref('stg_sap_adw_enderecos') }}
    )
    ,stg_estados as (
        select id_estado, cd_estado, nm_estado, cd_pais, id_territorio
        from {{ ref('stg_sap_adw_estados') }}
    )
    ,stg_territorios as (
        select * 
        from {{ ref('stg_sap_adw_vendas_territorios') }}
    )
    ,stg_paises as (
        select cd_pais, nm_pais 
        from {{ ref('stg_sap_adw_paises') }}
    )
    ,stg_entidades_enderecos as (
        select
            row_number() over(partition by id_entidade order by dt_modificacao desc, id_endereco desc) as linha
            ,entend.*
        from {{ ref('stg_sap_adw_entidades_enderecos') }} entend
    )
    ,stg_enderecos_tipos as (
        select *
        from {{ ref('stg_sap_adw_enderecos_tipos') }}
    )
    ,stg_entidades_contatos as (
        select 
            row_number() over(partition by id_entidade order by dt_modificacao desc, id_contato desc) as linha
            ,entcont.*
        from {{ ref('stg_sap_adw_entidades_contatos') }} entcont
    )

    ,int_pessoas as (
        select *
        from {{ ref('int_pessoas_related') }} 
    )

    ,join_tables as (

        select 
            ent.id_entidade
            --DADOS DE ENDEREÇO DA ENTIDADE
            ,tipedr.id_tipo_endereco
            ,tipedr.nm_tipo_endereco
            ,edr.id_endereco
            ,edr.ds_endereco
            ,edr.nm_cidade
            ,est.id_estado
            ,est.cd_estado
            ,est.nm_estado
            ,edr.nr_cep
            ,est.cd_pais
            ,pai.nm_pais
            ,ter.nm_territorio
            ,ter.ds_grupo_territorial
            --DADOS DE CONTATO DA ENTIDADE
            ,con.ds_email_primario
            ,con.ds_email_secundario
            ,con.nm_tipos_telefones
            ,con.nr_telefones
            ,ent.dt_modificacao
        from stg_entidades ent 
        left join stg_entidades_enderecos entedr on entedr.id_entidade = ent.id_entidade and entedr.linha = 1
        left join stg_entidades_contatos entcon on entcon.id_entidade = ent.id_entidade and entcon.linha = 1
        left join stg_enderecos edr on edr.id_endereco = entedr.id_endereco
        left join stg_estados est on est.id_estado = edr.id_estado
        left join stg_territorios ter on ter.id_territorio = est.id_territorio
        left join stg_paises pai on pai.cd_pais = est.cd_pais
        left join stg_enderecos_tipos tipedr on tipedr.id_tipo_endereco = entedr.id_tipo_endereco
        left join int_pessoas con on con.id_pessoa = entcon.id_contato
        
    )  


select * from join_tables


