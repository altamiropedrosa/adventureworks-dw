with
    stg_pessoas as (
        select *
        from {{ ref('stg_sap_adw_pessoas') }}
    )
    ,stg_emails as (
        select *
        from {{ ref('stg_sap_adw_pessoas_emails') }}
    )  
    ,int_telefones as (
        select *
        from {{ ref('int_telefones_joins') }} 
    )
    ,int_entidades as (
        select *
        from {{ ref('int_entidades_joins') }}
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
            ,inttel.nm_tipos_telefones
            ,inttel.nr_telefones
            ,intent.*
        from stg_pessoas pes 
        left join stg_emails ema on ema.id_pessoa = pes.id_pessoa
        left join int_telefones inttel on inttel.id_pessoa = pes.id_pessoa
        left join int_entidades intent on intent.id_entidade = pes.id_pessoa
    )  

select * from stg_pessoas --where id_pessoa = 1704

