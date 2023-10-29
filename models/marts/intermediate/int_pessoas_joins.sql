with
    stg_pessoas as (
        select *
        from {{ ref('stg_sap_adw_pessoas') }}
    )
    ,stg_pessoas_telefones as (
        select *
        from {{ ref('stg_sap_adw_pessoas_telefones') }}
    )
    ,stg_tipos_telefones as (
        select *
        from {{ ref('stg_sap_adw_tipos_telefones') }}
    )
    ,stg_emails as (
        select *
        from {{ ref('stg_sap_adw_emails') }}
    )  
    ,stg_entidades_negocios_enderecos as (
        select *
        from {{ ref('stg_sap_adw_entidades_negocios_enderecos') }}
    )
    ,int_enderecos as (
        select *
        from {{ ref('int_enderecos_joins') }}
    )  
    ,stg_tipos_enderecos as (
        select *
        from {{ ref('stg_sap_adw_tipos_enderecos') }}
    )  

    ,join_tables as (
        select 
            pes.id_pessoa
            ,pes.cd_tipo_pessoa
            ,pes.nm_pessoa
            ,pes.cd_email_promocional     
            ,ema.ds_email
            ,pes.ds_contato_adicional
            ,pes.ds_dados_demograficos
            ,tiptel.nm_tipo_telefone
            ,tel.nr_telefone
            ,tipedr.nm_tipo_endereco
            ,edr.*
        from stg_pessoas pes 
        left join stg_emails ema on pes.id_pessoa = ema.id_pessoa
        left join stg_pessoas_telefones tel on tel.id_pessoa = ema.id_pessoa
        left join stg_tipos_telefones tiptel on tiptel.id_tipo_telefone = tel.id_tipo_telefone
        left join stg_entidades_negocios_enderecos ent on ent.id_entidade_negocio = pes.id_pessoa
        left join int_enderecos edr on edr.id_endereco = ent.id_endereco
        left join stg_tipos_enderecos tipedr on tipedr.id_tipo_endereco = ent.id_tipo_endereco

    )  

select * from join_tables    