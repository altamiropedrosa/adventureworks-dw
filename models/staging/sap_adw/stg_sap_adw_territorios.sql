with 

source as (

    select * from {{ source('sap_adw', 'salesterritory') }}

),

renamed as (

    select
        territoryid as id_territorio
        ,name as nm_territorio
        ,countryregioncode as cd_pais
        ,`group` as ds_grupo_territorio
        ,salesytd as vl_venda_ytd
        ,saleslastyear as vl_venda_ultimo_ano
        ,costytd as vl_custo_ytd
        ,costlastyear as vl_custo_ultimo_ano
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
