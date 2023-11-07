with
    stg_pessoas as (
        select *
        from {{ ref('stg_sap_adw_pessoas') }}
    )
    ,stg_pessoas_emails as (
        select 
            row_number() over(partition by id_pessoa order by dt_modificacao desc, id_email desc) as linha
            ,ema.*
        from {{ ref('stg_sap_adw_pessoas_emails') }} ema
    )  
    ,int_telefones as (
        select *
        from {{ ref('int_telefones_related') }} 
    )


    ,join_tables as (
        select 
            pes.id_pessoa
            ,pes.cd_tipo_pessoa
            ,pes.nm_pessoa
            ,pes.cd_email_promocional     
            ,emapri.ds_email as ds_email_primario
            ,emasec.ds_email as ds_email_secundario
            ,inttel.nm_tipos_telefones
            ,inttel.nr_telefones
            ,pes.ds_contato_adicional
            ,pes.ds_dados_demograficos
            ,pes.dt_modificacao
        from stg_pessoas pes 
        left join stg_pessoas_emails emapri on emapri.id_pessoa = pes.id_pessoa and emapri.linha = 1
        left join stg_pessoas_emails emasec on emasec.id_pessoa = pes.id_pessoa and emasec.linha = 2
        left join int_telefones inttel on inttel.id_pessoa = pes.id_pessoa
    )  

select * from join_tables --where id_pessoa = 1704