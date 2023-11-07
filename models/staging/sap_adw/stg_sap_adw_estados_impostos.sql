with 

    source as (

        select * from {{ source('sap_adw', 'salestaxrate') }}

    )

    ,renamed as (

        select
            cast(salestaxrateid as int) as id_imposto
            ,cast(stateprovinceid as int) as id_estado
            ,case when taxtype = 1 then 'Varejo'
                when taxtype = 2 then 'Atacado'
                when taxtype = 3 then 'Atacado & Varejo'
            end as cd_tipo_imposto
            ,coalesce(round(cast(taxrate as numeric),2),0) as pc_imposto           
            ,trim(name) as nm_imposto
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
