with 

    source as (

        select * from {{ source('sap_adw', 'businessentityaddress') }}

    )

    ,renamed as (

        select
            cast(businessentityid as int) as id_entidade
            ,cast(addressid as int) as id_endereco
            ,cast(addresstypeid as int) as id_tipo_endereco
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed

