with 
    stg_territorio as (
        select * from {{ ref('stg_sap_adw_salesterritory') }}
    )
    ,stg_pais as (
        select * from {{ ref('stg_sap_adw_countryregion') }}
    )    
    
    ,join_tables as (
        select 
            ter.territoryid as nk_id_territorio
            ,ter.name as nome_territorio
            ,ter.countryregioncode as sigla_pais
            ,pai.name as nome_pais 
            ,ter.grupo as grupo_territorio
            ,round(ter.salesytd,2) as total_vendas_ytd
            ,round(ter.saleslastyear,2) as total_vendas_ultimo_ano
            ,round(ter.costytd,2) as total_custo_ytd
            ,round(ter.costlastyear,2) as total_custo_ultimo_ano
        from stg_territorio ter
        left join stg_pais pai on pai.countryregioncode = ter.countryregioncode
    )

    ,refined as (
        select 
            row_number() over(order by nk_id_territorio) as sk_territorio
            ,join_tables.*
        from join_tables
    )

select * from refined 