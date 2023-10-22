with 
    stg_categoria_produtos as (
        select * from {{ ref('stg_sap_adw_produtos_categorias') }}
    )
    ,stg_subcategoria_produtos as (
        select * from {{ ref('stg_sap_adw_produtos_subcategorias') }}
    )
    ,stg_modelo_produtos as (
        select * from {{ ref('stg_sap_adw_produtos_modelos') }}
    )
    ,stg_unidademedida_produtos as (
        select * from {{ ref('stg_sap_adw_unidades_medidas') }}
    )
    ,stg_produtos as (
        select * from {{ ref('stg_sap_adw_produtos') }}
    )

    ,join_tables as (
        select 
            prd.id_produto
            ,prd.nm_produto
            ,prd.nr_produto
            ,prd.is_producao_propria
            ,prd.is_item_vendavel
            ,prd.ds_cor
            ,prd.qt_minima_estoque
            ,prd.qt_minima_reabastecimento
            ,round(prd.vl_custo_producao,2) as vl_custo_producao
            ,round(prd.vl_venda,2) as vl_venda
            ,prd.nr_tamanho
            ,prd.nr_peso
            ,prd.cd_unidade_medida_tamanho
            ,undtam.nm_unidade_medida as nm_unidade_medida_tamanho
            ,prd.cd_unidade_medida_peso
            ,undpes.nm_unidade_medida as nm_unidade_medida_peso
            ,prd.qt_dias_para_fabricacao
            ,prd.cd_linha_produto
            ,case when upper(prd.cd_linha_produto) = 'R' then 'ESTRADA'
                when upper(prd.cd_linha_produto) = 'M' then 'MOUNTANHA'
                when upper(prd.cd_linha_produto) = 'T' then 'TURISMO'
                when upper(prd.cd_linha_produto) = 'S' then 'PADRAO'
                else null
            end ds_linha_produto
            ,case when upper(prd.cd_classificacao) = 'H' then 'ALTO'
                when upper(prd.cd_classificacao) = 'M' then 'MEDIO'
                when upper(prd.cd_classificacao) = 'L' then 'BAIXO'
                else null
            end as cd_classificacao
            ,case when upper(prd.cd_estilo) = 'W' then 'MULHER'
                when upper(prd.cd_estilo) = 'M' then 'HOMEM'
                when upper(prd.cd_estilo) = 'U' then 'UNIVERSAL'
                else null
            end as cd_estilo
            ,cat.id_categoria_produto
            ,cat.nm_categoria_produto
            ,prd.id_subcategoria_produto
            ,sub.nm_subcategoria_produto
            ,prd.id_modelo_produto
            ,mod.nm_modelo_produto
            ,prd.dt_inicio_venda
            ,prd.dt_fim_venda
            ,prd.dt_descontinuado
        from stg_produtos prd
        left join stg_subcategoria_produtos sub on sub.id_subcategoria_produto = prd.id_subcategoria_produto
        left join stg_categoria_produtos cat on cat.id_categoria_produto = sub.id_categoria_produto
        left join stg_modelo_produtos mod on mod.id_modelo_produto = prd.id_modelo_produto
        left join stg_unidademedida_produtos undtam on undtam.cd_unidade_medida = prd.cd_unidade_medida_tamanho
        left join stg_unidademedida_produtos undpes on undpes.cd_unidade_medida = prd.cd_unidade_medida_peso
        where prd.id_produto = 771
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_produto']) }} as sk_produto
            ,join_tables.*        
        from join_tables
    )

select * from refined