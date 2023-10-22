with 

source as (

    select * from {{ source('sap_adw', 'personphone') }}

),

renamed as (

    select
        businessentityid as id_pessoa
        ,phonenumber nr_telefone
        ,phonenumbertypeid as id_tipo_telefone
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
