with 
    stg_enderecos as (
        select * 
        from {{ ref('stg_sap_adw_enderecos') }}
    )
    ,stg_estados as (
        select id_estado, cd_estado, nm_estado, cd_pais, id_territorio
        from {{ ref('stg_sap_adw_estados') }}
    )
    ,stg_paises as (
        select cd_pais, nm_pais 
        from {{ ref('stg_sap_adw_paises') }}
    )


    ,join_tables as (
        select 
            edr.id_endereco
            ,edr.ds_endereco
            ,edr.nm_cidade
            ,edr.id_estado
            ,est.cd_estado
            ,est.nm_estado
            ,edr.nr_cep
            ,est.cd_pais
            ,pai.nm_pais
            ,edr.dt_modificacao
        from stg_enderecos edr
        left join stg_estados est on est.id_estado = edr.id_estado
        left join stg_paises pai on pai.cd_pais = est.cd_pais
    )

select * from join_tables