with 

source as (

    select * from {{ source('sap_adw', 'personcreditcard') }}

),

renamed as (

    select
        businessentityid
        ,creditcardid
        ,modifieddate

    from source

)

select * from renamed
