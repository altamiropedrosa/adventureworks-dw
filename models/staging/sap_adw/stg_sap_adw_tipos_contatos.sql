with 

source as (

    select * from {{ source('sap_adw', 'contacttype') }}

),

renamed as (

    select
        contacttypeid as id_tipo_contato
        ,name as nm_tipo_contato
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
