with 

    source as (

        select * from {{ source('sap_adw', 'currencyrate') }}

    )

    ,renamed as (

        select
            cast(currencyrateid as int) as id_taxa_cambio
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(currencyratedate as timestamp)) as timestamp) as dt_taxa_cambio        
            ,trim(tocurrencycode) as cd_moeda_de_taxa_cambio
            ,trim(fromcurrencycode) as cd_moeda_para_taxa_cambio
            ,round(cast(averagerate as numeric),4) as vl_medio_taxa_cambio
            ,round(cast(endofdayrate as numeric),4) as vl_fechamento_taxa_cambio
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
