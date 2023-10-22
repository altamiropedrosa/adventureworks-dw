with 

source as (

    select * from {{ source('sap_adw', 'productsubcategory') }}

),

renamed as (

    select
        productsubcategoryid as id_subcategoria_produto
        ,productcategoryid as id_categoria_produto
        ,name as nm_subcategoria_produto
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
