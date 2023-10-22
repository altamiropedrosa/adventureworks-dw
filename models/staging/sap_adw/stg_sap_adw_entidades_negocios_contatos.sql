with 

source as (

    select * from {{ source('sap_adw', 'businessentitycontact') }}

),

renamed as (

    select
        businessentityid as id_entidade_negocio
        ,personid as id_pessoa
        ,contacttypeid as id_tipo_contato
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
