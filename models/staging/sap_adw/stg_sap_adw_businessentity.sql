with 

source as (

    select * from {{ source('sap_adw', 'businessentity') }}

),

renamed as (

    select
        businessentityid
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
