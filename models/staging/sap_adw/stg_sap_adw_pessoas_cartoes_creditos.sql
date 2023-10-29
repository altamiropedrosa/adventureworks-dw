with 

    source as (

        select * from {{ source('sap_adw', 'personcreditcard') }}

    )

    ,renamed as (

        select
            cast(businessentityid as int) as id_pessoa
            ,cast(creditcardid as int) as id_cartao_credito
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
