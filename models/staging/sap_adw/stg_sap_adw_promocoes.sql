with 

    source as (

        select * from {{ source('sap_adw', 'specialoffer') }}

    )

    ,renamed as (

        select
            cast(specialofferid as int) as id_promocao
            ,trim(description) as ds_promocao
            ,coalesce(round(cast(discountpct as numeric),3),0.0) as pc_desconto
            ,trim(type) as ds_tipo_promocao
            ,trim(category) as ds_categoria_promocao
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(startdate as timestamp)) as timestamp) as dt_inicio_promocao        
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(enddate as timestamp)) as timestamp) as dt_fim_promocao        
            ,coalesce(cast(minqty as int),0) as qt_minima
            ,coalesce(cast(maxqty as int),99999) as qt_maxima
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
