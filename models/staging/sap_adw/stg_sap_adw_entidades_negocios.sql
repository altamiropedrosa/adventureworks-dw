with 

    source as (

        select * from {{ source('sap_adw', 'businessentity') }}

    )

    ,renamed as (

        select
            cast(businessentityid as int) as id_entidade_negocio
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
