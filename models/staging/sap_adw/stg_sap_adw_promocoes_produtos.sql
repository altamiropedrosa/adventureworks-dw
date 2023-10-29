with 

    source as (

        select * from {{ source('sap_adw', 'specialofferproduct') }}

    )

    ,renamed as (

        select
            cast(specialofferid as int) as id_promocao
            ,cast(productid as int) as id_produto
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
