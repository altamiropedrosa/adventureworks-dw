with 
    stg_cliente_cartao_credito as (
        select * from {{ ref('stg_sap_adw_personcreditcard') }}
    )
    ,stg_cartao_credito as (
        select * from {{ ref('stg_sap_adw_creditcard') }}
    )
    ,stg_pessoa as (
        select * from {{ ref('stg_sap_adw_person') }}
    )

    ,join_tables as (
        select 
            car.creditcardid
            ,car.cardtype
            ,car.cardnumber
            ,car.expmonth
            ,car.expyear
            ,carpes.businessentityid as nk_id_pessoa_cliente
            ,case when carpes.businessentityid is null then 'NI' 
                else concat(case when pes.title is null then '' else pes.title end,' ',
                            case when pes.firstname is null then '' else pes.firstname end,' ',
                            case when pes.middlename is null then '' else pes.middlename end,' ',
                            case when pes.lastname is null then '' else pes.lastname end) 
            end as nome_responsavel
        from stg_cartao_credito car
        left join stg_cliente_cartao_credito carpes on carpes.creditcardid = car.creditcardid
        left join stg_pessoa pes on pes.businessentityid = carpes.businessentityid
    )

    ,refined as (
        select 
            row_number() over(order by creditcardid) as sk_cartao_credito
            ,{{ dbt_utils.generate_surrogate_key(['creditcardid','cardnumber']) }} as pk_cartao_credito
            ,join_tables.*
        from join_tables
    )

select * from refined 