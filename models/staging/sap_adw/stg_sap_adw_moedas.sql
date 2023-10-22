with 

source as (

    select * from {{ source('sap_adw', 'currency') }}

),

renamed as (

    select
        currencycode as cd_moeda
        ,name as nm_moeda
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
