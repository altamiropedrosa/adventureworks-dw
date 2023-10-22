with 

source as (

    select * from {{ source('sap_adw', 'businessentityaddress') }}

),

renamed as (

    select
        businessentityid as id_entidade_negocio
        ,addressid as id_endereco
        ,addresstypeid as id_tipo_endereco
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
