with 

    source as (

        select * from {{ source('sap_adw', 'salesperson') }}

    )

    ,renamed as (

        select
            cast(businessentityid as int) as id_vendedor
            ,cast(territoryid as int) as id_territorio
            ,coalesce(round(cast(salesquota as numeric),2),0) as vl_cota_vendas
            ,coalesce(round(cast(bonus as numeric),2),0) as vl_bonus
            ,coalesce(round(cast(commissionpct as numeric),3),0.0) as pc_comissao
            ,coalesce(round(cast(salesytd as numeric),2),0) as vl_vendas_ytd
            ,coalesce(round(cast(saleslastyear as numeric),2),0) as vl_vendas_ultimo_ano
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
