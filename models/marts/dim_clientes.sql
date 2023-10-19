with 
    stg_cliente as (
        select * from {{ ref('stg_sap_adw_customer') }}
    )
    ,stg_pessoa as (
        select * from {{ ref('stg_sap_adw_person') }}
    )
    ,stg_territorio as (
        select * from {{ ref('stg_sap_adw_salesterritory') }}
    )
    ,stg_pais as (
        select * from {{ ref('stg_sap_adw_countryregion') }}
    )    
    ,stg_loja as (
        select * from {{ ref('stg_sap_adw_store') }}
    )
    ,stg_telefone as (
        select * from {{ ref('stg_sap_adw_personphone') }}
    )
    ,stg_tipo_telefone as (
        select * from {{ ref('stg_sap_adw_phonenumbertype') }}
    )

    ,join_tables as (
        select 
            cli.customerid as nk_id_cliente
            ,cli.personid as nk_id_pessoa_cliente
            ,case when cli.personid is null then 'NI' 
                else concat(case when pescli.title is null then '' else pescli.title end,' ',
                            case when pescli.firstname is null then '' else pescli.firstname end,' ',
                            case when pescli.middlename is null then '' else pescli.middlename end,' ',
                            case when pescli.lastname is null then '' else pescli.lastname end) 
            end as nome_cliente
            ,pescli.emailpromotion as email_promocional
            ,cli.storeid as nk_id_loja
            ,loj.name as nome_loja
            ,loj.salespersonid as nk_id_pessoa_vendedor
            ,case when loj.salespersonid is null then 'NI' 
                else concat(case when pesven.title is null then '' else pesven.title end,' ',
                            case when pesven.firstname is null then '' else pesven.firstname end,' ',
                            case when pesven.middlename is null then '' else pesven.middlename end,' ',
                            case when pesven.lastname is null then '' else pesven.lastname end) 
            end as nome_vendedor
            ,cli.territoryid as nk_id_territorio
            ,ter.name as nome_territorio
            ,ter.grupo as grupo_territorio
            --
            ,ter.countryregioncode as sigla_pais
            ,pai.name as nome_pais 
            --
            ,tel.phonenumber as telefone_cliente
            ,tiptel.name as tipo_telefone
        from stg_cliente cli
        left join stg_pessoa pescli on pescli.businessentityid = cli.personid
        left join stg_territorio ter on ter.territoryid = cli.territoryid
        left join stg_pais pai on pai.countryregioncode = ter.countryregioncode
        left join stg_loja loj on loj.businessentityid = cli.storeid
        left join stg_pessoa pesven on pesven.businessentityid = loj.salespersonid
        left join stg_telefone tel on tel.businessentityid = cli.personid
        left join stg_tipo_telefone tiptel on tiptel.phonenumbertypeid = tel.phonenumbertypeid
    )

    ,refined as (
        select 
            row_number() over(order by nk_id_cliente) as sk_cliente
            ,join_tables.*
        from join_tables
    )

select * from refined 