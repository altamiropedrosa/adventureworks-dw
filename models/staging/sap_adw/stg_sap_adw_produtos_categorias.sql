with 

source as (

    select * from {{ source('sap_adw', 'productcategory') }}

),

renamed as (

    select
        productcategoryid as id_categoria_produto
        ,name as nm_categoria_produto
        ,rowguid
        ,modifieddate as dt_modificao

    from source

)

select * from renamed
