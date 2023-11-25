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
    ,stg_pessoas_telefones as (
        select *
        from {{ ref('stg_sap_adw_pessoas_telefones') }}
    )
    ,stg_tipos_telefones as (
        select *
        from {{ ref('stg_sap_adw_tipos_telefones') }}
    )

    ,array_telefones as (

        select 
            tel.id_pessoa
            ,string_agg(tiptel.nm_tipo_telefone, ' | ') as nm_tipos_telefones
            ,string_agg(tel.nr_telefone, ' | ') as nr_telefones
        from stg_pessoas_telefones tel 
        inner join stg_tipos_telefones tiptel on tiptel.id_tipo_telefone = tel.id_tipo_telefone
        group by id_pessoa

    )

    ,join_tables as (

        select 
            pes.id_pessoa
            ,pes.cd_tipo_pessoa
            ,pes.nm_pessoa
            ,pes.cd_email_promocional     
            ,emapri.ds_email as ds_email_primario
            ,emasec.ds_email as ds_email_secundario
            ,tel.nm_tipos_telefones
            ,tel.nr_telefones
            ,pes.ds_contato_adicional
            ,pes.ds_dados_demograficos
            ,pes.dt_modificacao
        from stg_pessoas pes 
        left join stg_pessoas_emails emapri on emapri.id_pessoa = pes.id_pessoa and emapri.linha = 1
        left join stg_pessoas_emails emasec on emasec.id_pessoa = pes.id_pessoa and emasec.linha = 2
        left join array_telefones tel on tel.id_pessoa = pes.id_pessoa
        
    )  


select * from join_tables