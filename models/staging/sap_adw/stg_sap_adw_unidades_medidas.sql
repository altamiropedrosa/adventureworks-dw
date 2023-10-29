with 

    source as (

        select * from {{ source('sap_adw', 'unitmeasure') }}

    )

    ,renamed as (

        select
            trim(unitmeasurecode) as cd_unidade_medida
            ,trim(name) as nm_unidade_medida
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
