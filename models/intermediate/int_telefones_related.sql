with 
    stg_pessoas_telefones as (
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


select * from  array_telefones --where id_pessoa = 1704