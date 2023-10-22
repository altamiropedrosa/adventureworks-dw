with 

source as (

    select * from {{ source('sap_adw', 'countryregion') }}

),

renamed as (

    select
        countryregioncode as cd_pais
        ,name as nm_pais
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
