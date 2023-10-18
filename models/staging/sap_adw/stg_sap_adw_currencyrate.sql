with 

source as (

    select * from {{ source('sap_adw', 'currencyrate') }}

),

renamed as (

    select
        currencyrateid
        ,currencyratedate
        ,fromcurrencycode
        ,tocurrencycode
        ,averagerate
        ,endofdayrate
        ,modifieddate

    from source

)

select * from renamed
