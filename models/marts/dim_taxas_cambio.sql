with 
    stg_taxas_cambio as (
        select * from {{ ref('stg_sap_adw_taxas_cambio') }}
    )
    ,stg_moedas as (
        select * from {{ ref('stg_sap_adw_moedas') }}
    )

    ,join_tables as (
        select 
            tax.id_taxa_cambio
            ,tax.dt_taxa_cambio
            ,tax.cd_moeda_de
            ,moede.nm_moeda as nm_moeda_de
            ,tax.cd_moeda_para
            ,moepara.nm_moeda as nm_moeda_para
            ,tax.vl_medio
            ,tax.vl_fechamento
        from stg_taxas_cambio tax
        left join stg_moedas moede on moede.cd_moeda = tax.cd_moeda_de
        left join stg_moedas moepara on moepara.cd_moeda = tax.cd_moeda_para
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_taxa_cambio']) }} as sk_taxa_cambio
            ,join_tables.*
        from join_tables
    )

select * from refined 