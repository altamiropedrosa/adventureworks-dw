with 
    stg_vendas_territorios as (
        select * 
        from {{ ref('stg_sap_adw_vendas_territorios') }}
    )
    ,stg_paises as (
        select cd_pais, nm_pais 
        from {{ ref('stg_sap_adw_paises') }}
    )
    ,stg_paises_moedas as (
        select * from {{ ref('stg_sap_adw_paises_moedas') }}
        where cd_pais not in ('AT','BE','ES','FI','GR','IE','IT','NL','PT','DE','FR','PL','RU')

        union all

        select * from {{ ref('stg_sap_adw_paises_moedas') }}
        where cd_pais in ('AT','BE','ES','FI','GR','IE','IT','NL','PT','DE','FR','PL','RU') and cd_moeda = 'EUR'
    )
    ,stg_moedas as (
        select cd_moeda, nm_moeda
        from {{ ref('stg_sap_adw_moedas') }}
    )


    ,join_tables as (
        select 
            ter.id_territorio
            ,ter.nm_territorio
            ,ter.cd_pais
            ,pai.nm_pais
            ,moe.cd_moeda
            ,moe.nm_moeda
            ,ter.ds_grupo_territorial
            ,ter.vl_venda_ytd
            ,ter.vl_venda_ultimo_ano
            ,ter.vl_custo_ytd
            ,ter.vl_custo_ultimo_ano
            ,ter.dt_modificacao
        from stg_vendas_territorios ter
        left join stg_paises pai on pai.cd_pais = ter.cd_pais
        left join stg_paises_moedas paimoe on paimoe.cd_pais = pai.cd_pais
        left join stg_moedas moe on moe.cd_moeda = paimoe.cd_moeda
    )

select * from join_tables