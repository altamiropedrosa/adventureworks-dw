with 

    source as (

        select * from {{ source('sap_adw', 'address') }}

    )

    ,renamed as (

        select
            cast(addressid as int) as id_endereco
            ,concat(addressline1,' ',coalesce(addressline2,'')) as ds_endereco
            ,trim(city) as nm_cidade
            ,cast(stateprovinceid as int) as id_estado
            ,postalcode as nr_cep
            ,spatiallocation as ds_dados_geograficos
            ,rowguid as rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
