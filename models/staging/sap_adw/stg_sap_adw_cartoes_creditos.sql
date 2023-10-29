with 

    source as (

        select * from {{ source('sap_adw', 'creditcard') }}

    )

    ,renamed as (

        select
            cast(creditcardid as int) as id_cartao_credito
            ,trim(cardtype) as cd_tipo_cartao
            ,cast(cardnumber as int)as nr_cartao
            ,cast(expmonth as int) as nr_expiracao_mes
            ,cast(expyear as int) as nr_expiracao_ano
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao 

        from source

    )

select * from renamed
