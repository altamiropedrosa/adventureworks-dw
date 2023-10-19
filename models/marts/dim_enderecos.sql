with 
    stg_provincia as (
        select * from {{ ref('stg_sap_adw_stateprovince') }}
    )
    ,stg_pais as (
        select * from {{ ref('stg_sap_adw_countryregion') }}
    )
    ,stg_territorio as (
        select * from {{ ref('stg_sap_adw_salesterritory') }}
    )
    ,stg_endereco as (
        select * from {{ ref('stg_sap_adw_address') }}
    )


    ,join_tables as (
        select 
            edr.addressid
            ,edr.addressline1
            ,edr.addressline2
            ,edr.city
            ,edr.stateprovinceid
            ,edr.postalcode
            ,edr.spatiallocation
            --
            ,prv.stateprovincecode
            ,prv.countryregioncode
            ,prv.name as provincia
            ,prv.territoryid
            --
            ,ter.name as territorio
            ,ter.grupo
            --
            ,pai.name as pais
        from stg_endereco edr
        left join stg_provincia prv on prv.stateprovinceid = edr.stateprovinceid
        left join stg_territorio ter on ter.territoryid = prv.territoryid
        left join stg_pais pai on pai.countryregioncode = prv.countryregioncode
    )

    ,refined as (
        select 
            row_number() over(order by addressid) as sk_endereco
            ,addressid as nk_id_endereco
            ,concat(addressline1,' ',case when addressline2 is null then '' else addressline2 end) as endereco
            ,city as cidade
            ,stateprovinceid as nk_id_provincia
            ,postalcode as cep
            ,spatiallocation as localizacao_espacial
            ,stateprovincecode 
            ,countryregioncode
            ,provincia
            ,territoryid as nk_id_territorio
            ,territorio
            ,grupo as grupo_territorio
            ,pais        
        from join_tables
    )

select * from refined 