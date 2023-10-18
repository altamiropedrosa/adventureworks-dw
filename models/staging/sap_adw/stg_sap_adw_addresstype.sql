with 

source as (

    select * from {{ source('sap_adw', 'addresstype') }}

),

renamed as (

    select
        addresstypeid
        ,name
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
