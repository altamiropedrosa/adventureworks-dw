with 

source as (

    select * from {{ source('sap_adw', 'salesreason') }}

),

renamed as (

    select
        salesreasonid
        ,name
        ,reasontype
        ,modifieddate

    from source

)

select * from renamed
