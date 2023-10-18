with 
    stg_categoria_produtos (
        select * from {{ ref('stg_sap_adw_productcategory') }}
    )
    ,stg_subcategoria_produtos (
        select * from {{ ref('stg_sap_adw_productsubcategory') }}
    )
    ,stg_modelo_produtos (
        select * from {{ ref('stg_sap_adw_productmodel') }}
    )