with 
    stg_ofertas as (
        select * from {{ ref('stg_sap_adw_ofertas') }}
    )
    ,stg_ofertas_produtos as (
        select * from {{ ref('stg_sap_adw_ofertas_produtos') }}
    )
    ,stg_produtos as (
        select * from {{ ref('stg_sap_adw_produtos') }}
    )
    ,stg_produtos_categorias as (
        select * from {{ ref('stg_sap_adw_produtos_categorias') }}
    )
    ,stg_produtos_subcategorias as (
        select * from {{ ref('stg_sap_adw_produtos_subcategorias') }}
    )

    
    ,join_tables as (
        select 
            ofe.id_oferta
            ,ofe.ds_oferta
            ,case when ofe.pc_desconto is null then 0.0 else ofe.pc_desconto end as pc_desconto
            ,ofe.ds_tipo_oferta
            ,ofe.ds_categoria_oferta
            ,ofe.dt_inicio_oferta
            ,ofe.dt_fim_oferta
            ,case when ofe.qt_minima is null then 0 else ofe.qt_minima end as qt_minima
            ,case when ofe.qt_maxima is null then 0 else ofe.qt_maxima end as qt_maxima
            ,ofepro.id_produto
            ,prd.nm_produto
            ,sub.nm_subcategoria_produto
            ,cat.nm_categoria_produto
        from stg_ofertas ofe
        left join stg_ofertas_produtos ofepro on ofepro.id_oferta = ofe.id_oferta
        left join stg_produtos prd on prd.id_produto = ofepro.id_produto
        left join stg_produtos_subcategorias sub on sub.id_subcategoria_produto = prd.id_subcategoria_produto
        left join stg_produtos_categorias cat on cat.id_categoria_produto = sub.id_categoria_produto
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_oferta']) }} as sk_oferta
            ,join_tables.*
        from join_tables
    )

select * from refined 