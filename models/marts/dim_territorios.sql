with 
    int_territorios as (
        select 
            id_territorio
            ,nm_territorio
            ,cd_pais
            ,nm_pais
            ,cd_moeda
            ,nm_moeda
            ,ds_grupo_territorial
            ,vl_venda_ytd
            ,vl_venda_ultimo_ano
            ,vl_custo_ytd
            ,vl_custo_ultimo_ano
            ,dt_modificacao
        from {{ ref('int_territorios_related') }}
    )
    
    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_territorio']) }} as sk_territorio
            ,int_territorios.*
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from int_territorios
    )

select * from refined