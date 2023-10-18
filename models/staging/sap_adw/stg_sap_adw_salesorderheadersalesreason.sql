with 

source as (

    select * from {{ source('sap_adw', 'salesorderheadersalesreason') }}

),

renamed as (

    select
        salesorderid
        ,salesreasonid
        ,modifieddate

    from source

)

select * from renamed
