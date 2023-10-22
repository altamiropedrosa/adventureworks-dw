with 

source as (

    select * from {{ source('sap_adw', 'addresstype') }}

),

renamed as (

    select
        addresstypeid as id_tipo_endereco
        ,name as nm_tipo_endereco
        ,rowguid
        ,modifieddate as dt_alteracao

    from source

)

select * from renamed
