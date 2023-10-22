with 

source as (

    select * from {{ source('sap_adw', 'salesreason') }}

),

renamed as (

    select
        salesreasonid as id_razao_venda
        ,name as nm_razao_venda
        ,reasontype as cd_tipo_razao
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
