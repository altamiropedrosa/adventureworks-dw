with source as (

    select * from {{ source('sap_adw', 'product') }}

),

renamed as (

    select
        cast(productid as int) as id_produto
        ,name as nm_produto
        ,productnumber nr_produto
        ,makeflag as is_producao_propria
        ,finishedgoodsflag as is_item_vendavel
        ,color as ds_cor
        ,safetystocklevel as qt_minima_estoque
        ,reorderpoint as qt_minima_reabastecimento
        ,standardcost as vl_custo_producao
        ,listprice as vl_venda
        ,size as nr_tamanho
        ,sizeunitmeasurecode as cd_unidade_medida_tamanho
        ,weightunitmeasurecode as cd_unidade_medida_peso
        ,weight as nr_peso
        ,daystomanufacture as qt_dias_para_fabricacao
        ,productline as cd_linha_produto
        ,class as cd_classificacao
        ,style as cd_estilo
        ,productsubcategoryid as id_subcategoria_produto
        ,productmodelid as id_modelo_produto
        ,sellstartdate as dt_inicio_venda
        ,sellenddate as dt_fim_venda
        ,discontinueddate as dt_descontinuado
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
