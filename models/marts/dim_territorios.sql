with 
    stg_territorios as (
        select * from {{ ref('stg_sap_adw_territorios') }}
    )
    ,stg_paises as (
        select * from {{ ref('stg_sap_adw_paises') }}
    )    
    
    ,join_tables as (
        select 
            ter.id_territorio
            ,ter.nm_territorio
            ,ter.cd_pais
            ,pai.nm_pais 
            ,ter.ds_grupo_territorio
            ,round(ter.vl_venda_ytd,2) as vl_venda_ytd
            ,round(ter.vl_venda_ultimo_ano,2) as vl_venda_ultimo_ano
            ,round(ter.vl_custo_ytd,2) as vl_custo_ytd
            ,round(ter.vl_custo_ultimo_ano,2) as vl_custo_ultimo_ano
        from stg_territorios ter
        left join stg_paises pai on pai.cd_pais = ter.cd_pais
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_territorio']) }} as sk_territorio
            ,join_tables.*
        from join_tables
    )

select * from refined 