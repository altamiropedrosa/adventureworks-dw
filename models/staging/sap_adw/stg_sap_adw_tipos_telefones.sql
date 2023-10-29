with 

    source as (

        select * from {{ source('sap_adw', 'phonenumbertype') }}

    )
    
    ,renamed as (

        select
            cast(phonenumbertypeid as int) as id_tipo_telefone
            ,trim(name) as nm_tipo_telefone
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
