with 

source as (

    select * from {{ source('sap_adw', 'store') }}

),

renamed as (

    select
        businessentityid,
        name,
        salespersonid,
        demographics,
        rowguid,
        modifieddate

    from source

)

select * from renamed
