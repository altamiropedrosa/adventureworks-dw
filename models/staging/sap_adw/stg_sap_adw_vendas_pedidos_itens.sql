with 

    source as (

        select * from {{ source('sap_adw', 'salesorderdetail') }}

    )

    ,renamed as (

        select
            cast(salesorderid as int) as id_pedido_venda
            ,cast(salesorderdetailid as int) as id_pedido_venda_item
            ,trim(carriertrackingnumber) as nr_rastreamento
            ,coalesce(cast(orderqty as int),0) as qt_pedido_item
            ,cast(productid as int) as id_produto
            ,cast(specialofferid as int) as id_promocao
            ,coalesce(cast(unitprice as BIGNUMERIC),0) as vl_unitario_item
            ,coalesce(cast(unitpricediscount as BIGNUMERIC),0) as pc_desconto_item
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed where id_pedido_venda = 46607






/*select sum(total) as total 110.373.889.31
from
(
select id_pedido_venda, sum(vl_unitario*qt_pedido) as total 
from renamed
group by id_pedido_venda
)*/

