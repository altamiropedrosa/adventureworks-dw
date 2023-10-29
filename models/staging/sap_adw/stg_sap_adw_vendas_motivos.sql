with 

    source as (

        select * from {{ source('sap_adw', 'salesreason') }}

    )

    ,renamed as (

        select
            cast(salesreasonid as int) as id_motivo_venda
            ,trim(name) as nm_motivo_venda
            ,trim(reasontype) as cd_motivo_razao
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
