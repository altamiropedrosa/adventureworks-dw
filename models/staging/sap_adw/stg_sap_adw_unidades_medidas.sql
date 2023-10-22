with 

source as (

    select * from {{ source('sap_adw', 'unitmeasure') }}

),

renamed as (

    select
        unitmeasurecode as cd_unidade_medida
        ,name as nm_unidade_medida
        ,modifieddate as dt_modificacao

    from source

)

select * from renamed
