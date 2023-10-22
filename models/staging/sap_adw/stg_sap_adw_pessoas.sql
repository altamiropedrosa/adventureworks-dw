with 

source as (

    select * from {{ source('sap_adw', 'person') }}

),

renamed as (

    select
        businessentityid as id_pessoa
        ,persontype as cd_tipo_pessoa
        ,namestyle as is_estilo_oriental
        ,title as ds_titulo
        ,firstname as ds_primeiro_nome
        ,middlename as ds_nome_meio
        ,lastname as ds_ultimo_nome
        ,suffix as cd_sufixo_nome
        ,emailpromotion as cd_email_promocional
        ,additionalcontactinfo as ds_contato_adicional
        ,demographics as ds_dados_demograficos
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
