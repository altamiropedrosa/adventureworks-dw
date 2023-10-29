with 

    source as (

        select * from {{ source('sap_adw', 'businessentitycontact') }}

    )

    ,renamed as (

        select
            cast(businessentityid as int) as id_entidade_negocio
            ,cast(personid as int) as id_pessoa
            ,cast(contacttypeid as int) as id_tipo_contato
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
