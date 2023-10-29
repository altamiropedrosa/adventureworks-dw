with 

    source as (

        select * from {{ source('sap_adw', 'countryregion') }}

    )

    ,renamed as (

        select
            trim(countryregioncode) as cd_pais
            ,trim(name) as nm_pais
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
