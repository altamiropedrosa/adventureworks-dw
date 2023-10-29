with 

    source as (

        select * from {{ source('sap_adw', 'stateprovince') }}

    )

    ,renamed as (

        select
            cast(stateprovinceid as int) as id_estado
            ,trim(stateprovincecode) as cd_estado
            ,trim(countryregioncode) as cd_pais
            ,cast(isonlystateprovinceflag as boolean) as is_estado_pais
            ,trim(name) as nm_estado
            ,cast(territoryid as int) as id_territorio
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
