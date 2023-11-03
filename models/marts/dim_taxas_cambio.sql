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
            ,tax.cd_moeda_de_taxa_cambio
            ,moede.nm_moeda as nm_moeda_de_taxa_cambio
            ,tax.cd_moeda_para_taxa_cambio
            ,moepara.nm_moeda as nm_moeda_para_taxa_cambio
            ,tax.vl_medio_taxa_cambio
            ,tax.vl_fechamento_taxa_cambio
            ,tax.dt_modificacao
        from stg_taxas_cambio tax
        left join stg_moedas moede on moede.cd_moeda = tax.cd_moeda_de_taxa_cambio
        left join stg_moedas moepara on moepara.cd_moeda = tax.cd_moeda_para_taxa_cambio
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_taxa_cambio']) }} as sk_taxa_cambio
            ,join_tables.*
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from join_tables
    )

select * from refined 

