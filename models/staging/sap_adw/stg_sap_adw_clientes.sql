with 

    source as (

        select * from {{ source('sap_adw', 'customer') }}

    ),

    renamed as (

        select
            cast(customerid as int) as id_cliente
            ,cast(personid as int) as id_pessoa
            ,cast(storeid as int) as id_loja
            ,cast(territoryid as int) as id_territorio
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao 
        from source

    )

select * from renamed --where id_cliente in (29484,585)
