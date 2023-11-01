with
    stg_entidades as (
        select *
        from {{ ref('stg_sap_adw_entidades') }}
    )
    ,stg_entidades_enderecos as (
        select *
        from {{ ref('stg_sap_adw_entidades_enderecos') }}
    )
    ,stg_entidades_contatos as (
        select *
        from {{ ref('stg_sap_adw_entidades_contatos') }}
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
        from {{ ref('int_enderecos_joins') }}
    )  
    --,int_pessoas as (
    --    select *
    --    from {{ ref('int_pessoas_joins') }} 
    --)


    ,join_tables as (
        select 
            ent.id_entidade

            ,entcon.id_contato
            --,intpes.nm_pessoa as nm_contato

            ,tipcon.id_tipo_contato
            ,tipcon.nm_tipo_contato

            ,tipedr.id_tipo_endereco
            ,tipedr.nm_tipo_endereco

            ,intedr.*
        from stg_entidades ent 
        left join stg_entidades_enderecos entedr on entedr.id_entidade = ent.id_entidade
        left join stg_enderecos_tipos tipedr on tipedr.id_tipo_endereco = entedr.id_tipo_endereco
        left join stg_entidades_contatos entcon on entcon.id_entidade = ent.id_entidade
        left join stg_tipos_contatos tipcon on tipcon.id_tipo_contato = entcon.id_tipo_contato
        --left join int_pessoas intpes on intpes.id_pessoa = entcon.id_contato
        left join int_enderecos intedr on intedr.id_endereco = entedr.id_endereco
    )  

select * from join_tables
