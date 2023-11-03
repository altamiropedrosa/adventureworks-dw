with 
    stg_cartoes_creditos as (
        select * from {{ ref('stg_sap_adw_cartoes_creditos') }}
    )
    ,stg_pessoas_cartoes_creditos as (
        select * from {{ ref('stg_sap_adw_pessoas_cartoes_creditos') }}
    )
    ,int_pessoas as (
        select id_pessoa, nm_pessoa from {{ ref('int_pessoas_related') }}
    )

    ,join_tables as (
        select 
            pescar.id_pessoa as id_responsavel
            ,pes.nm_pessoa as nm_responsavel
            ,pescar.id_cartao_credito
            ,car.cd_tipo_cartao_credito
            ,car.nr_cartao_credito
            ,car.nr_expiracao_mes_cartao_credito
            ,car.nr_expiracao_ano_cartao_credito
            ,pescar.dt_modificacao
        from stg_pessoas_cartoes_creditos pescar
        left join stg_cartoes_creditos car on car.id_cartao_credito = pescar.id_cartao_credito
        left join int_pessoas pes on pes.id_pessoa = pescar.id_pessoa
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_cartao_credito','id_responsavel']) }} as sk_cartao_credito
            ,join_tables.*
            ,cast(format_timestamp('%Y-%m-%d %H:%M:%S', current_timestamp, 'America/Sao_Paulo') as timestamp) as dt_carga
        from join_tables
    )


select * from refined 
