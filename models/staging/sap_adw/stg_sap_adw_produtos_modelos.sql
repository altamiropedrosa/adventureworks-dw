with 

    source as (

        select * from {{ source('sap_adw', 'productmodel') }}

    ),

    renamed as (

        select
            cast(productmodelid as int) as id_modelo_produto
            ,trim(name) as nm_modelo_produto
            ,case when catalogdescription = '[NULL]' then null else catalogdescription end as ds_catalogo
            ,case when instructions = '[NULL]' then null else instructions end as ds_instrucoes
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
