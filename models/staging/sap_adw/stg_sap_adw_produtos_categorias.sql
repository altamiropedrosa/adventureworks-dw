with 

    source as (

        select * from {{ source('sap_adw', 'productcategory') }}

    )

    ,renamed as (

        select
            cast(productcategoryid as int) as id_categoria_produto
            ,trim(name) as nm_categoria_produto
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
