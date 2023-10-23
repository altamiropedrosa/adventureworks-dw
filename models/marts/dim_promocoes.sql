with 
    stg_promocoes as (
        select * from {{ ref('stg_sap_adw_promocoes') }}
    )
    
    ,join_tables as (
        select 
            pro.id_promocao
            ,pro.ds_promocao
            ,case when pro.pc_desconto is null then 0.0 else pro.pc_desconto end as pc_desconto
            ,pro.ds_tipo_promocao
            ,pro.ds_categoria_promocao
            ,pro.dt_inicio_promocao
            ,pro.dt_fim_promocao
            ,case when pro.qt_minima is null then 0 else pro.qt_minima end as qt_minima
            ,case when pro.qt_maxima is null then 0 else pro.qt_maxima end as qt_maxima
        from stg_promocoes pro
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_promocao']) }} as sk_promocao
            ,join_tables.*
        from join_tables
    )

select * from refined 