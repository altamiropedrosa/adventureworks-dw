with 

source as (

    select * from {{ source('sap_adw', 'shipmethod') }}

),

renamed as (

    select
        shipmethodid as id_forma_envio
        ,name as nm_forma_envio
        ,shipbase as vl_minimo_envio
        ,shiprate as vl_envio_por_kg
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
