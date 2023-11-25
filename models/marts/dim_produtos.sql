with 
    stg_produtos as (
        select * from {{ ref('stg_sap_adw_produtos') }}
    )
    ,stg_categoria_produtos as (
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
            ,round(prd.vl_custo_produto,2) as vl_custo_produto
            ,round(prd.vl_tabela_produto,2) as vl_tabela_produto
            ,prd.nr_tamanho
            ,prd.nr_peso
            ,prd.cd_unidade_medida_tamanho
            ,undtam.nm_unidade_medida as nm_unidade_medida_tamanho
            ,prd.cd_unidade_medida_peso
            ,undpes.nm_unidade_medida as nm_unidade_medida_peso
            ,prd.qt_dias_para_fabricacao
            ,prd.cd_linha_produto
            ,prd.cd_classificacao_produto
            ,prd.cd_estilo_produto
            ,cat.id_categoria_produto
            ,cat.nm_categoria_produto
            ,prd.id_subcategoria_produto
            ,sub.nm_subcategoria_produto
            ,prd.id_modelo_produto
            ,mod.nm_modelo_produto
            ,prd.dt_inicio_venda
            ,prd.dt_fim_venda
            ,prd.is_descontinuado
            ,prd.dt_modificacao
        from stg_produtos prd
        left join stg_subcategoria_produtos sub on sub.id_subcategoria_produto = prd.id_subcategoria_produto
        left join stg_categoria_produtos cat on cat.id_categoria_produto = sub.id_categoria_produto
        left join stg_modelo_produtos mod on mod.id_modelo_produto = prd.id_modelo_produto
        left join stg_unidademedida_produtos undtam on undtam.cd_unidade_medida = prd.cd_unidade_medida_tamanho
        left join stg_unidademedida_produtos undpes on undpes.cd_unidade_medida = prd.cd_unidade_medida_peso
        
    )

    ,refined as (

        select 
            {{ dbt_utils.generate_surrogate_key(['id_produto']) }} as sk_produto
            ,join_tables.*     
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from join_tables
        
    )


select * from refined


