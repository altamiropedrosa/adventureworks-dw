with 

source as (

    select * from {{ source('sap_adw', 'personcreditcard') }}

),

renamed as (

    select
        businessentityid as id_pessoa
        ,creditcardid as id_cartao_credito
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
