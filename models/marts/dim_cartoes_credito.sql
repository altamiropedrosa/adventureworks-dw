with 
    stg_pessoas_cartoes_creditos as (
        select * from {{ ref('stg_sap_adw_pessoas_cartoes_creditos') }}
    )
    ,stg_cartoes_creditos as (
        select * from {{ ref('stg_sap_adw_cartoes_creditos') }}
    )
    ,stg_pessoas as (
        select * from {{ ref('stg_sap_adw_pessoas') }}
    )

    ,join_tables as (
        select 
            car.id_cartao_credito
            ,car.cd_tipo_cartao
            ,car.nr_cartao
            ,car.nr_expiracao_mes
            ,car.nr_expiracao_ano
            ,carpes.id_pessoa as id_responsavel
            ,case when carpes.id_pessoa is null then 'Não Identificado' 
                else concat(case when pes.ds_titulo is null then '' else pes.ds_titulo end,' ',
                            case when pes.ds_primeiro_nome is null then '' else pes.ds_primeiro_nome end,' ',
                            case when pes.ds_nome_meio is null then '' else pes.ds_nome_meio end,' ',
                            case when pes.ds_ultimo_nome is null then '' else pes.ds_ultimo_nome end) 
            end as nm_responsavel
        from stg_cartoes_creditos car
        left join stg_pessoas_cartoes_creditos carpes on carpes.id_cartao_credito = car.id_cartao_credito
        left join stg_pessoas pes on pes.id_pessoa = carpes.id_pessoa
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_cartao_credito','nr_cartao']) }} as sk_cartao_credito
            ,join_tables.*
        from join_tables
    )

select * from refined 