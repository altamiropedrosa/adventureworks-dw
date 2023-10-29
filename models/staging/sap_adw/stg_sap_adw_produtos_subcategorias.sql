with 

    source as (

        select * from {{ source('sap_adw', 'productsubcategory') }}

    )

    ,renamed as (

        select
            cast(productsubcategoryid as int) as id_subcategoria_produto
            ,cast(productcategoryid as int) as id_categoria_produto
            ,trim(name) as nm_subcategoria_produto
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
