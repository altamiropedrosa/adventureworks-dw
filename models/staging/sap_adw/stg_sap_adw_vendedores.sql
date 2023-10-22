with 

source as (

    select * from {{ source('sap_adw', 'salesperson') }}

),

renamed as (

    select
        businessentityid as id_vendedor
        ,territoryid as id_territorio
        ,salesquota as vl_cota_vendas
        ,bonus as vl_bonus
        ,commissionpct as pc_comissao
        ,salesytd as vl_vendas_ytd
        ,saleslastyear as vl_vendas_ultimo_ano
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
