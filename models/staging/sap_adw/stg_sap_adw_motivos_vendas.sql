with 

source as (

    select * from {{ source('sap_adw', 'salesreason') }}

),

renamed as (

    select
        salesreasonid as id_motivo_venda
        ,name as nm_motivo_venda
        ,reasontype as cd_motivo_razao
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
