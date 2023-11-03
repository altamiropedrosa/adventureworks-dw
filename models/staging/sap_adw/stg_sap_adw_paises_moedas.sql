with 

    source as (

        select * from {{ source('sap_adw', 'countryregioncurrency') }}

    )

    ,renamed as (

        select
            trim(countryregioncode) as cd_pais
            ,trim(currencycode) as cd_moeda
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
