with 

    source as (

        select * from {{ source('sap_adw', 'salesorderheader') }}

    )


    ,renamed as (
        select
            cast(salesorderid as int) as id_pedido_venda
            ,cast(revisionnumber as int) as nr_revisao_pedido
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(orderdate as timestamp)) as timestamp) as dt_pedido
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(duedate as timestamp)) as timestamp) as dt_pagamento
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(shipdate as timestamp)) as timestamp) as dt_envio
            ,case when status = 1 then 'Em processo'
                when status = 2 then 'Aprovado'
                when status = 3 then 'Em espera'
                when status = 4 then 'Rejeitado'
                when status = 5 then 'Enviado'
                when status = 6 then 'Cancelado'
                else 'Em processo'
            end as cd_status
            ,cast(onlineorderflag as boolean) as is_pedido_realizado_pelo_cliente
            ,trim(purchaseordernumber) as nr_ordem_compra
            ,trim(accountnumber) as nr_conta_financeira
            ,cast(customerid as int) as id_cliente
            ,cast(salespersonid as int) as id_vendedor
            ,cast(territoryid as int) as id_territorio
            ,cast(billtoaddressid as int) as id_endereco_cobranca
            ,cast(shiptoaddressid as int) as id_endereco_entrega
            ,cast(shipmethodid as int) as id_forma_envio
            ,cast(creditcardid as int) as id_cartao_credito
            ,trim(creditcardapprovalcode) as cd_aprovacao_cartao_credito
            ,cast(currencyrateid as int) as id_taxa_cambio
            ,coalesce(cast(subtotal as BIGNUMERIC),0) as vl_subtotal
            ,coalesce(cast(taxamt as BIGNUMERIC),0) as vl_imposto
            ,coalesce(cast(freight as BIGNUMERIC),0) as vl_frete
            ,coalesce(cast(totaldue as BIGNUMERIC),0) as vl_total
            ,comment as ds_comentario
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source
    )

select * from renamed

