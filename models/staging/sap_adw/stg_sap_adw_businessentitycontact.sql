with 

source as (

    select * from {{ source('sap_adw', 'businessentitycontact') }}

),

renamed as (

    select
        businessentityid
        ,personid
        ,contacttypeid
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
