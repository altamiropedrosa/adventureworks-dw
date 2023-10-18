with 

source as (

    select * from {{ source('sap_adw', 'customer') }}

),

renamed as (

    select
        customerid as pk_cliente
        ,personid
        ,storeid
        ,territoryid
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
