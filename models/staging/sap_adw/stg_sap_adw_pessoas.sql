with 

    source as (

        select * from {{ source('sap_adw', 'person') }}

    )

    ,renamed as (

        select
            cast(businessentityid as int) as id_pessoa
            ,case when persontype = 'SC' then 'Contato Loja'
                when persontype = 'IN' then 'Varejo'
                when persontype = 'SP' then 'Vendedor'
                when persontype = 'EM' then 'Funcionário'
                when persontype = 'VC' then 'Contato Fornecedor'
                when persontype = 'GC' then 'Contato Geral'
            end as cd_tipo_pessoa
            ,cast(namestyle as boolean) as is_estilo_oriental
            ,concat(coalesce(title,''),' ',
                    coalesce(firstname,''),' ',
                    coalesce(middlename,''),' ',
                    coalesce(lastname,''),' ',
                    coalesce(suffix,'')) as nm_pessoa
            ,case when emailpromotion = 0 then 'Não Contatar'
                when emailpromotion = 1 then 'Contatar por Adw'
                when emailpromotion = 2 then 'Contatar por Adw + Parceiros'
            end as cd_email_promocional     
            ,case when additionalcontactinfo = '[NULL]' then null else additionalcontactinfo end as ds_contato_adicional
            ,demographics as ds_dados_demograficos
            ,rowguid
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as dt_modificacao        

        from source

    )


select * from renamed
