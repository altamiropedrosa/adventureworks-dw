with 

source as (

    select * from {{ source('sap_adw', 'specialoffer') }}

),

renamed as (

    select
        specialofferid as id_promocao
        ,description as ds_promocao
        ,discountpct as pc_desconto
        ,type as ds_tipo_promocao
        ,category as ds_categoria_promocao
        ,startdate as dt_inicio_promocao
        ,enddate as dt_fim_promocao
        ,minqty as qt_minima
        ,maxqty as qt_maxima
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
