with 

    source as (

        select * from {{ source('sap_adw', 'addresstype') }}

    )

    ,renamed as (

        select
            cast(addresstypeid as int) as id_tipo_endereco
            ,trim(name) as nm_tipo_endereco
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
