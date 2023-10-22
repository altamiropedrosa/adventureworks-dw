with 

source as (

    select * from {{ source('sap_adw', 'salesorderdetail') }}

),

renamed as (

    select
        salesorderid as id_pedido_venda
        ,salesorderdetailid as id_pedido_venda_item
        ,carriertrackingnumber as nr_rastreamento
        ,orderqty as qt_pedido
        ,productid as id_produto
        ,specialofferid as id_oferta
        ,unitprice as vl_unitario
        ,unitpricediscount as vl_desconto
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
