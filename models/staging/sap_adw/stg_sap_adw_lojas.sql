with 

source as (

    select * from {{ source('sap_adw', 'store') }}

),

renamed as (

    select
        businessentityid as id_loja
        ,name as nm_loja
        ,salespersonid as id_vendedor
        ,demographics as ds_dados_demograficos
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
