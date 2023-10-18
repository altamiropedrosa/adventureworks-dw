with 

source as (

    select * from {{ source('sap_adw', 'salesorderheader') }}

),

renamed as (

    select
        salesorderid
        ,revisionnumber
        ,orderdate
        ,duedate
        ,shipdate
        ,status
        ,onlineorderflag
        ,purchaseordernumber
        ,accountnumber
        ,customerid
        ,salespersonid
        ,territoryid
        ,billtoaddressid
        ,shiptoaddressid
        ,shipmethodid
        ,creditcardid
        ,creditcardapprovalcode
        ,currencyrateid
        ,subtotal
        ,taxamt
        ,freight
        ,totaldue
        ,comment
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
