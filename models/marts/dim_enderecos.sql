with 
    int_enderecos as (
        select  
            id_endereco
            ,ds_endereco
            ,nm_cidade
            ,id_estado
            ,cd_estado
            ,nm_estado
            ,nr_cep
            ,cd_pais
            ,nm_pais
            ,dt_modificacao       
        from {{ ref('int_enderecos_related') }}
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_endereco']) }} as sk_endereco
            ,int_enderecos.*
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from int_enderecos
    )

select * from refined 