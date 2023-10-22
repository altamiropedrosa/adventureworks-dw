with 

source as (

    select * from {{ source('sap_adw', 'customer') }}

),

renamed as (

    select
        customerid as id_cliente
        ,personid as id_pessoa
        ,storeid as id_loja
        ,territoryid as id_territorio
        ,rowguid
        ,modifieddate as dt_alteracao

    from source

)

select * from renamed
