with 

source as (

    select * from {{ source('sap_adw', 'unitmeasure') }}

),

renamed as (

    select
        unitmeasurecode
        ,name
        ,modifieddate

    from source

)

select * from renamed
