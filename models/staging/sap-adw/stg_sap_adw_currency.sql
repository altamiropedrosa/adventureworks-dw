with 

source as (

    select * from {{ source('sap_adw', 'currency') }}

),

renamed as (

    select
        currencycode
        ,name
        ,modifieddate

    from source

)

select * from renamed
