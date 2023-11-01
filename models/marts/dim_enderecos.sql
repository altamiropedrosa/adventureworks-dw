with 
    int_enderecos as (
        select * from {{ ref('int_enderecos_joins') }}
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_endereco']) }} as sk_endereco
            ,edr.*
        from int_enderecos edr
    )

select * from refined 