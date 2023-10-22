with 

source as (

    select * from {{ source('sap_adw', 'currencyrate') }}

),

renamed as (

    select
        currencyrateid as id_taxa_cambio
        ,currencyratedate as dt_taxa_cambio
        ,fromcurrencycode as cd_moeda_para
        ,tocurrencycode as cd_moeda_de
        ,averagerate as vl_medio
        ,endofdayrate as vl_fechamento
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
