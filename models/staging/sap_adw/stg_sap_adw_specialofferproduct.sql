with 

source as (

    select * from {{ source('sap_adw', 'specialofferproduct') }}

),

renamed as (

    select
        specialofferid
        ,productid
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
