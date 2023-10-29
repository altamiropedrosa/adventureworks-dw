with 

    source as (

        select * from {{ source('sap_adw', 'emailaddress') }}

    )

    ,renamed as (

        select
            cast(businessentityid as int) as id_pessoa
            ,cast(emailaddressid as int) as id_email
            ,lower(trim(emailaddress)) as ds_email
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
