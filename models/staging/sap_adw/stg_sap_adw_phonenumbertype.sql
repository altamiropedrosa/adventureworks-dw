with 

source as (

    select * from {{ source('sap_adw', 'phonenumbertype') }}

),

renamed as (

    select
        phonenumbertypeid
        ,name
        ,modifieddate

    from source

)

select * from renamed
