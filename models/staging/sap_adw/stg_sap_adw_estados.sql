with 

source as (

    select * from {{ source('sap_adw', 'stateprovince') }}

),

renamed as (

    select
        stateprovinceid as id_estado
        ,stateprovincecode cd_estado
        ,countryregioncode as cd_pais
        ,isonlystateprovinceflag as is_estado_pais
        ,name as nm_estado
        ,territoryid id_territorio
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
