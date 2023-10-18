with 

source as (

    select * from {{ source('sap_adw', 'salesorderdetail') }}

),

renamed as (

    select
        salesorderid
        ,salesorderdetailid
        ,carriertrackingnumber
        ,orderqty
        ,productid
        ,specialofferid
        ,unitprice
        ,unitpricediscount
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
