with 

    source as (

        select * from {{ source('sap_adw', 'personphone') }}

    )

    ,renamed as (

        select
            cast(businessentityid as int) as id_pessoa
            ,trim(replace(replace(replace(replace(replace(phonenumber,'-',''),',',''),')',''),'(',''),' ','')) as nr_telefone
            ,cast(phonenumbertypeid as int) as id_tipo_telefone
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )

select * from renamed
