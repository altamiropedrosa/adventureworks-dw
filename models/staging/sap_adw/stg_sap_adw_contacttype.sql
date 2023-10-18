with 

source as (

    select * from {{ source('sap_adw', 'contacttype') }}

),

renamed as (

    select
        contacttypeid
        ,name
        ,modifieddate

    from source

)

select * from renamed
