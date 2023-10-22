with 

source as (

    select * from {{ source('sap_adw', 'creditcard') }}

),

renamed as (

    select
        creditcardid as id_cartao_credito
        ,cardtype as cd_tipo_cartao
        ,cardnumber as nr_cartao
        ,expmonth as nr_expiracao_mes
        ,expyear as nr_expiracao_ano
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
