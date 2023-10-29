with 

    source as (

        select * from {{ source('sap_adw', 'contacttype') }}

    )

    ,renamed as (

        select
            cast(contacttypeid as int) as id_tipo_contato
            ,trim(name) as nm_tipo_contato
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
