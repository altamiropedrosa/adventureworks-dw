with 

source as (

    select * from {{ source('sap_adw', 'specialoffer') }}

),

renamed as (

    select
        specialofferid as id_oferta
        ,description as ds_oferta
        ,discountpct as pc_desconto
        ,type as ds_tipo_oferta
        ,category as ds_categoria_oferta
        ,startdate as dt_inicio_oferta
        ,enddate as dt_fim_oferta
        ,minqty as qt_minima
        ,maxqty as qt_maxima
        ,rowguid
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
