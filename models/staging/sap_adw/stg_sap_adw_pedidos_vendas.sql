with 

source as (

    select * from {{ source('sap_adw', 'salesorderheader') }}

),

renamed as (

    select
        salesorderid as id_pedido_venda
        ,revisionnumber as nr_revisao
        ,orderdate as dt_pedido
        ,duedate as dt_pagamento
        ,shipdate as dt_envio
        ,status as cd_status
        ,onlineorderflag as in_pedido_online
        ,purchaseordernumber as nr_ordem_compra
        ,accountnumber as nr_conta_financeira
        ,customerid as id_cliente
        ,salespersonid as id_vendedor
        ,territoryid as id_territorio
        ,billtoaddressid as id_endereco_cobranca
        ,shiptoaddressid as id_endereco_entrega
        ,shipmethodid as id_forma_envio
        ,creditcardid as id_cartao_credito
        ,creditcardapprovalcode as cd_aprovacao_cartao_credito
        ,currencyrateid as id_taxa_cambio
        ,subtotal as vl_subtotal
        ,taxamt as vl_imposto
        ,freight as vl_frete
        ,totaldue as vl_pago
        ,comment as ds_comentario
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
