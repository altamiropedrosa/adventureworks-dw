with 
    stg_pessoas as (
        select * from {{ ref('stg_sap_adw_pessoas') }}
    )
    ,stg_vendedores as (
        select * from {{ ref('stg_sap_adw_vendedores') }}
    )
    ,stg_vendas_territorios as (
        select * from {{ ref('stg_sap_adw_vendas_territorios') }}
    )
    ,stg_paises as (
        select * from {{ ref('stg_sap_adw_paises') }}
    )    


    ,join_tables as (
        select 
            ven.id_vendedor
            ,pes.nm_pessoa as nm_vendedor
            ,ven.vl_cota_vendas
            ,ven.vl_bonus
            ,ven.pc_comissao
            ,ven.vl_vendas_ytd
            ,ven.vl_vendas_ultimo_ano
            ,ven.id_territorio
            ,ter.nm_territorio
            ,ter.ds_grupo_territorio
            ,ter.cd_pais
            ,pai.nm_pais 
        from stg_vendedores ven
        left join stg_pessoas pes on pes.id_pessoa = ven.id_vendedor
        left join stg_vendas_territorios ter on ter.id_territorio = ven.id_territorio
        left join stg_paises pai on pai.cd_pais = ter.cd_pais
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_vendedor']) }} as sk_vendedor
            ,join_tables.*
        from join_tables
    )

select * from refined 