with 

    source as (

        select * from {{ source('sap_adw', 'product') }}

    )

    ,renamed as (

        select
            cast(productid as int) as id_produto
            ,trim(name) as nm_produto
            ,trim(productnumber) as nr_produto
            ,cast(makeflag as boolean) as is_producao_propria
            ,cast(finishedgoodsflag as boolean) as is_item_vendavel
            ,trim(color) as ds_cor
            ,cast(safetystocklevel as int) as qt_minima_estoque
            ,cast(reorderpoint as int) as qt_minima_reabastecimento
            ,coalesce(round(cast(standardcost as numeric),2),0) as vl_custo_produto
            ,coalesce(round(cast(listprice as numeric),2),0) as vl_tabela_produto
            ,trim(size) as nr_tamanho
            ,trim(sizeunitmeasurecode) as cd_unidade_medida_tamanho
            ,trim(weightunitmeasurecode) as cd_unidade_medida_peso
            ,coalesce(round(cast(weight as numeric),2),0.0) as nr_peso
            ,coalesce(cast(daystomanufacture as int),0) as qt_dias_para_fabricacao
            ,case when upper(productline) = 'R' then 'Estrada'
                when upper(productline) = 'M' then 'Montanha'
                when upper(productline) = 'T' then 'Turismo'
                when upper(productline) = 'S' then 'Padrão'
                else null
            end cd_linha_produto
            ,case when upper(class) = 'H' then 'Alto'
                when upper(class) = 'M' then 'Médio'
                when upper(class) = 'L' then 'Baixo'
                else null
            end as cd_classificacao_produto
            ,case when upper(style) = 'W' then 'Mulher'
                when upper(style) = 'M' then 'Homem'
                when upper(style) = 'U' then 'Universal'
                else null
            end as cd_estilo_produto 
            ,cast(productsubcategoryid as int) as id_subcategoria_produto
            ,cast(productmodelid as int) as id_modelo_produto
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(sellstartdate as timestamp)) as timestamp) as dt_inicio_venda        
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(sellenddate as timestamp)) as timestamp) as dt_fim_venda        
            ,case when discontinueddate is null then false else true end as is_descontinuado
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
