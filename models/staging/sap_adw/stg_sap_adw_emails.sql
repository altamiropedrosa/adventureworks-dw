with 

source as (

    select * from {{ source('sap_adw', 'emailaddress') }}

),

renamed as (

    select
        businessentityid as id_entidade_negocio
        ,emailaddressid as id_endereco_email
        ,emailaddress as ds_email
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
