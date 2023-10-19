with 

source as (

    select * from {{ source('sap_adw', 'creditcard') }}

),

renamed as (

    select
        creditcardid
        ,cardtype
        ,cardnumber
        ,expmonth
        ,expyear
        ,modifieddate

    from source

)

select * from renamed
