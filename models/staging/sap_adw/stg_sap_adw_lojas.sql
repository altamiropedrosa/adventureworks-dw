with 

    source as (

        select * from {{ source('sap_adw', 'store') }}

    )

    ,renamed as (

        select
            cast(businessentityid as int) as id_loja
            ,trim(name) as nm_loja
            ,cast(salespersonid as int) as id_vendedor
            ,demographics as ds_dados_demograficos
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed where id_loja = 298
