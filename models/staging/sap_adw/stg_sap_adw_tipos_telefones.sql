with 

source as (

    select * from {{ source('sap_adw', 'phonenumbertype') }}

),

renamed as (

    select
        phonenumbertypeid as id_tipo_telefone
        ,name as nm_tipo_telefone
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
