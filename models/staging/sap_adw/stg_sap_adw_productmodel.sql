with 

source as (

    select * from {{ source('sap_adw', 'productmodel') }}

),

renamed as (

    select
        productmodelid
        ,name
        ,catalogdescription
        ,instructions
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
