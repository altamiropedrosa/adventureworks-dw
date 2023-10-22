with 

source as (

    select * from {{ source('sap_adw', 'businessentity') }}

),

renamed as (

    select
        businessentityid as id_entidade_negocio
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
