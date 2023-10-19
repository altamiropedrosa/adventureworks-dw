with 
    stg_taxa_cambio as (
        select * from {{ ref('stg_sap_adw_currencyrate') }}
    )
    ,stg_moeda as (
        select * from {{ ref('stg_sap_adw_currency') }}
    )

    ,join_tables as (
        select 
            tax.currencyrateid as nk_id_taxa_cambio
            ,tax.currencyratedate as data_cambio
            ,tax.fromcurrencycode as codigo_moeda_de
            ,moede.name as nome_moeda_de
            ,tax.tocurrencycode as codigo_moeda_para
            ,moepara.name as nome_moeda_para
            ,tax.averagerate as taxa_media
            ,tax.endofdayrate as taxa_fim_dia
        from stg_taxa_cambio tax
        left join stg_moeda moede on moede.currencycode = tax.tocurrencycode
        left join stg_moeda moepara on moepara.currencycode = tax.fromcurrencycode
    )

    ,refined as (
        select 
            row_number() over(order by nk_id_taxa_cambio) as sk_taxa_cambio
            ,join_tables.*
        from join_tables
    )

select * from refined 