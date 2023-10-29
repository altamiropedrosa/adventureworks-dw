with 
    stg_estados as (
        select * from {{ ref('stg_sap_adw_estados') }}
    )
    ,stg_paises as (
        select * from {{ ref('stg_sap_adw_paises') }}
    )
    ,stg_vendas_territorios as (
        select * from {{ ref('stg_sap_adw_vendas_territorios') }}
    )
    ,stg_enderecos as (
        select * from {{ ref('stg_sap_adw_enderecos') }}
    )


    ,join_tables as (
        select 
            edr.id_endereco
            ,edr.ds_endereco
            ,edr.nm_cidade
            ,edr.id_estado
            ,prv.cd_estado
            ,prv.nm_estado
            ,edr.nr_cep
            ,edr.ds_dados_geograficos
            ,prv.cd_pais
            ,pai.nm_pais
            ,prv.id_territorio
            ,ter.nm_territorio
            ,ter.ds_grupo_territorio
        from stg_enderecos edr
        left join stg_estados prv on prv.id_estado = edr.id_estado
        left join stg_vendas_territorios ter on ter.id_territorio = prv.id_territorio
        left join stg_paises pai on pai.cd_pais = prv.cd_pais
    )


    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_endereco']) }} as sk_endereco
            ,join_tables.*
        from join_tables
    )

select * from refined 