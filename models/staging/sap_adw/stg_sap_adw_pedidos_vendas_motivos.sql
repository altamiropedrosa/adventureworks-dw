with 

source as (

    select * from {{ source('sap_adw', 'salesorderheadersalesreason') }}

),

renamed as (

    select
        salesorderid as id_pedido_venda
        ,salesreasonid as id_motivo_venda
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
