with 
    stg_promocoes_produtos as (
        select * from {{ ref('stg_sap_adw_promocoes_produtos') }}
    )
    ,stg_promocoes as (
        select * from {{ ref('stg_sap_adw_promocoes') }}
    )
    
    ,join_tables as (
        select 
            proprd.id_produto
            ,pro.id_promocao
            ,pro.nm_promocao
            ,pro.pc_desconto_promocao
            ,pro.ds_tipo_promocao
            ,pro.ds_categoria_promocao
            ,pro.dt_inicio_promocao
            ,pro.dt_fim_promocao
            ,pro.qt_minima_promocao
            ,pro.qt_maxima_promocao
            ,proprd.dt_modificacao
        from stg_promocoes_produtos proprd
        inner join stg_promocoes pro on pro.id_promocao = proprd.id_promocao
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_promocao','id_produto']) }} as sk_promocao
            ,join_tables.*
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from join_tables
    )

select * from refined 

