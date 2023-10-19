with 
    stg_pessoa as (
        select * from {{ ref('stg_sap_adw_person') }}
    )
    ,stg_vendedor as (
        select * from {{ ref('stg_sap_adw_salesperson') }}
    )
    ,stg_territorio as (
        select * from {{ ref('stg_sap_adw_salesterritory') }}
    )
    ,stg_pais as (
        select * from {{ ref('stg_sap_adw_countryregion') }}
    )    


    ,join_tables as (
        select 
            ven.businessentityid as nk_id_vendedor
            ,concat(case when pes.title is null then '' else pes.title end,' ',
                    case when pes.firstname is null then '' else pes.firstname end,' ',
                    case when pes.middlename is null then '' else pes.middlename end,' ',
                    case when pes.lastname is null then '' else pes.lastname end) as nome_vendedor
            ,ven.salesquota as cota_vendas
            ,ven.bonus as bonus_vendas
            ,ven.commissionpct as pct_comissao
            ,round(ven.salesytd,2) as total_vendas_ytd
            ,round(ven.saleslastyear,2) as total_vendas_ultimo_ano
            ,ven.territoryid as nk_id_territorio
            ,ter.name as nome_territorio
            ,ter.grupo as grupo_territorio
            ,ter.countryregioncode as sigla_pais
            ,pai.name as nome_pais 
        from stg_vendedor ven
        left join stg_pessoa pes on pes.businessentityid = ven.businessentityid
        left join stg_territorio ter on ter.territoryid = ven.territoryid
        left join stg_pais pai on pai.countryregioncode = ter.countryregioncode
    )

    ,refined as (
        select 
            row_number() over(order by nk_id_vendedor) as sk_vendedor
            ,join_tables.*
        from join_tables
    )

select * from refined 