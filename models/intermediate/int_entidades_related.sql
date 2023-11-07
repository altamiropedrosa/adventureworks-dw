with
    stg_entidades as (
        select *
        from {{ ref('stg_sap_adw_entidades') }}
    )
    ,stg_entidades_enderecos as (
        select
            row_number() over(partition by id_entidade order by dt_modificacao desc, id_endereco desc) as linha
            ,entend.*
        from {{ ref('stg_sap_adw_entidades_enderecos') }} entend
    )
    ,stg_entidades_contatos as (
        select 
            row_number() over(partition by id_entidade order by dt_modificacao desc, id_contato desc) as linha
            ,entcont.*
        from {{ ref('stg_sap_adw_entidades_contatos') }} entcont
    )
    ,stg_enderecos_tipos as (
        select *
        from {{ ref('stg_sap_adw_enderecos_tipos') }}
    )
    ,stg_tipos_contatos as (
        select *
        from {{ ref('stg_sap_adw_tipos_contatos') }}
    )
    ,int_enderecos as (
        select *
        from {{ ref('int_enderecos_related') }}
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
            ,edr.id_estado
            ,edr.cd_estado
            ,edr.nm_estado
            ,edr.nr_cep
            ,edr.cd_pais
            ,edr.nm_pais
            --DADOS DE CONTATO DA ENTIDADE
            ,tipcon.id_tipo_contato
            ,tipcon.nm_tipo_contato
            ,con.id_pessoa as id_contato
            ,con.nm_pessoa as nm_contato
            ,con.ds_email_primario as ds_email_primario_contato
            ,con.ds_email_secundario as ds_email_secundario_contato
            ,con.nm_tipos_telefones as nm_tipos_telefones_contato
            ,con.nr_telefones as nr_telefones_contato
            ,ent.dt_modificacao
        from stg_entidades ent 
        left join stg_entidades_enderecos entedr on entedr.id_entidade = ent.id_entidade and entedr.linha = 1
        left join stg_enderecos_tipos tipedr on tipedr.id_tipo_endereco = entedr.id_tipo_endereco
        left join int_enderecos edr on edr.id_endereco = entedr.id_endereco
        left join stg_entidades_contatos entcon on entcon.id_entidade = ent.id_entidade and entcon.linha = 1
        left join stg_tipos_contatos tipcon on tipcon.id_tipo_contato = entcon.id_tipo_contato
        left join int_pessoas con on con.id_pessoa = entcon.id_contato
    )  

select * from join_tables


