with 
    stg_categoria_produtos as (
        select * from {{ ref('stg_sap_adw_productcategory') }}
    )
    ,stg_subcategoria_produtos as (
        select * from {{ ref('stg_sap_adw_productsubcategory') }}
    )
    ,stg_modelo_produtos as (
        select * from {{ ref('stg_sap_adw_productmodel') }}
    )
    ,stg_unidademedida_produtos as (
        select * from {{ ref('stg_sap_adw_unitmeasure') }}
    )
    ,stg_produtos as (
        select * from {{ ref('stg_sap_adw_product') }}
    )

    ,join_tables as (
        select 
            prd.productid
            ,prd.name
            ,prd.productnumber
            ,prd.makeflag
            ,prd.finishedgoodsflag
            ,prd.color
            ,prd.safetystocklevel
            ,prd.reorderpoint
            ,prd.standardcost
            ,prd.listprice
            ,prd.size-- as tamanho
            ,prd.weight
            ,prd.daystomanufacture
            ,prd.productline
            ,prd.class
            ,prd.style
            ,prd.productsubcategoryid
            ,prd.productmodelid
            ,prd.sellstartdate
            ,prd.sellenddate
            ,prd.discontinueddate
            ,prd.rowguid
            ,prd.modifieddate
            --
            ,sub.name as subcategoria
            --
            ,cat.productcategoryid as id_categoria_nk 
            ,cat.name as categoria_produto
            --
            ,mod.name as modelo_produto
            --
            ,undtam.unitmeasurecode as sizeunitmeasurecode
            ,undtam.name as medida_tamanho
            --
            ,undpes.unitmeasurecode as weightunitmeasurecode
            ,undpes.name as medida_peso
        from stg_produtos prd
        left join stg_subcategoria_produtos sub on sub.productsubcategoryid = prd.productsubcategoryid
        left join stg_categoria_produtos cat on cat.productcategoryid = sub.productcategoryid
        left join stg_modelo_produtos mod on mod.productmodelid = prd.productmodelid
        left join stg_unidademedida_produtos undtam on undtam.unitmeasurecode = prd.sizeunitmeasurecode
        left join stg_unidademedida_produtos undpes on undpes.unitmeasurecode = prd.weightunitmeasurecode
    )

    ,refined as (
        select 
            row_number() over(order by productid) as sk_produto 
            ,productid as nk_id_produto
            ,name as nome_produto
            ,productnumber as numero_produto
            ,makeflag
            ,finishedgoodsflag
            ,color as cor
            ,safetystocklevel
            ,reorderpoint
            ,standardcost
            ,listprice
            ,size as tamanho
            ,weight as peso
            ,daystomanufacture
            ,productline
            ,class as classe_produto
            ,style
            ,productsubcategoryid as nk_id_subcategoria
            ,productmodelid as nk_id_modelo
            ,sellstartdate
            ,sellenddate
            ,discontinueddate
            ,subcategoria
            ,categoria_produto
            ,modelo_produto
            ,sizeunitmeasurecode
            ,medida_tamanho
            ,weightunitmeasurecode
            ,medida_peso
        
        from join_tables
    )

select * from refined    