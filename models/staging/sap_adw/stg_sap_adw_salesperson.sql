with 

source as (

    select * from {{ source('sap_adw', 'salesperson') }}

),

renamed as (

    select
        businessentityid
        ,territoryid
        ,salesquota
        ,bonus
        ,commissionpct
        ,salesytd
        ,saleslastyear
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
