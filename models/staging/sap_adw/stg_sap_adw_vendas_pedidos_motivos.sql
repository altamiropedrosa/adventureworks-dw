with 

    source as (

        select * from {{ source('sap_adw', 'salesorderheadersalesreason') }}

    )

    ,renamed as (

        select
            cast(salesorderid as int) as id_pedido_venda
            ,cast(salesreasonid as int) as id_motivo_venda
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
