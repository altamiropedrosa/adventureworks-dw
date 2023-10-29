with 
    stg_vendas_territorios as (
        select * from {{ ref('stg_sap_adw_vendas_territorios') }}
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
            ,ter.vl_venda_ytd
            ,ter.vl_venda_ultimo_ano
            ,ter.vl_custo_ytd
            ,ter.vl_custo_ultimo_ano
        from stg_vendas_territorios ter
        left join stg_paises pai on pai.cd_pais = ter.cd_pais
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_territorio']) }} as sk_territorio
            ,join_tables.*
        from join_tables
    )

select * from refined 