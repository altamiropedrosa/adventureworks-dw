with 

source as (

    select * from {{ source('sap_adw', 'countryregion') }}

),

renamed as (

    select
        countryregioncode
        ,name
        ,modifieddate

    from source

)

select * from renamed
