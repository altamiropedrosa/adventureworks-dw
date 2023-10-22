with 

source as (

    select * from {{ source('sap_adw', 'productmodel') }}

),

renamed as (

    select
        productmodelid as id_modelo_produto
        ,name as nm_modelo_produto
        ,catalogdescription as ds_catalogo
        ,instructions as ds_instrucoes
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
