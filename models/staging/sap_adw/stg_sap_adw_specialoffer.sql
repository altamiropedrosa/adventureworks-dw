with 

source as (

    select * from {{ source('sap_adw', 'specialoffer') }}

),

renamed as (

    select
        specialofferid
        ,description
        ,discountpct
        ,type
        ,category
        ,startdate
        ,enddate
        ,minqty
        ,maxqty
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
