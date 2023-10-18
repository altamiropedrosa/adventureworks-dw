with 

source as (

    select * from {{ source('sap_adw', 'emailaddress') }}

),

renamed as (

    select
        businessentityid
        ,emailaddressid
        ,emailaddress
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
