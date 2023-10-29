with 

    source as (

        select * from {{ source('sap_adw', 'shipmethod') }}

    )

    ,renamed as (

        select
            cast(shipmethodid as int) as id_forma_envio
            ,trim(name) as nm_forma_envio
            ,coalesce(round(cast(shipbase as numeric),2),1) as vl_envio_minimo
            ,coalesce(round(cast(shiprate as numeric),2),1) as vl_envio_por_kg
            ,rowguid
            ,format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as dt_modificacao 

        from source

    )

select * from renamed

