with 

source as (

    select * from {{ source('sap_adw', 'address') }}

),

renamed as (

    select
        addressid as id_endereco
        ,addressline1 as ds_endereco_1
        ,addressline2 as ds_endereco_2
        ,city as nm_cidade
        ,stateprovinceid as id_estado
        ,postalcode as nr_cep
        ,spatiallocation as ds_dados_geograficos
        ,rowguid as rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
