with 

source as (

    select * from {{ source('sap_adw', 'specialofferproduct') }}

),

renamed as (

    select
        specialofferid as id_oferta
        ,productid as id_produto
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
