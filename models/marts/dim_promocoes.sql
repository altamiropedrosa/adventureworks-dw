with 
    stg_promocoes as (
        select * from {{ ref('stg_sap_adw_promocoes') }}
    )
    
    ,join_tables as (
        select 
            pro.id_promocao
            ,pro.ds_promocao
            ,pro.pc_desconto
            ,pro.ds_tipo_promocao
            ,pro.ds_categoria_promocao
            ,pro.dt_inicio_promocao
            ,pro.dt_fim_promocao
            ,pro.qt_minima
            ,pro.qt_maxima
        from stg_promocoes pro
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_promocao']) }} as sk_promocao
            ,join_tables.*
        from join_tables
    )

select * from refined 