with 

source as (

    select * from {{ source('sap_adw', 'businessentityaddress') }}

),

renamed as (

    select
        businessentityid
        ,addressid
        ,addresstypeid
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
