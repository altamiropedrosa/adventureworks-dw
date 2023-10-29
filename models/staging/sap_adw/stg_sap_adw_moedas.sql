with 

    source as (

        select * from {{ source('sap_adw', 'currency') }}

    )

    ,renamed as (

        select
            trim(currencycode) as cd_moeda
            ,trim(name) as nm_moeda
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
