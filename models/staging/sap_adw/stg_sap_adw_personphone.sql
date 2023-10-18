with 

source as (

    select * from {{ source('sap_adw', 'personphone') }}

),

renamed as (

    select
        businessentityid
        ,phonenumber
        ,phonenumbertypeid
        ,modifieddate

    from source

)

select * from renamed
