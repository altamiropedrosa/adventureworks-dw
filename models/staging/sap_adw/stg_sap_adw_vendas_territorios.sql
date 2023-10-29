with 

    source as (

        select * from {{ source('sap_adw', 'salesterritory') }}

    )

    ,renamed as (

        select
            cast(territoryid as int) as id_territorio
            ,trim(name) as nm_territorio
            ,trim(countryregioncode) as cd_pais
            ,trim(`group`) as ds_grupo_territorio
            ,coalesce(round(cast(salesytd as numeric),2),0) as vl_venda_ytd
            ,coalesce(round(cast(saleslastyear as numeric),2),0) as vl_venda_ultimo_ano
            ,coalesce(round(cast(costytd as numeric),2),0) as vl_custo_ytd
            ,coalesce(round(cast(costlastyear as numeric),2),0) as vl_custo_ultimo_ano
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
