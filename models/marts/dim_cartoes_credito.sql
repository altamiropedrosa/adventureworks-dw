with 
    stg_cartoes_creditos as (
        select * from {{ ref('stg_sap_adw_cartoes_creditos') }}
    )
    ,stg_pessoas_cartoes_creditos as (
        select * from {{ ref('stg_sap_adw_pessoas_cartoes_creditos') }}
    )
    --,int_pessoas as (
    --    select id_pessoa, nm_pessoa from {{ ref('int_pessoas_joins') }}
    --)

    ,join_tables as (
        select 
            car.id_cartao_credito
            ,car.cd_tipo_cartao_credito
            ,car.nr_cartao_credito
            ,car.nr_expiracao_mes_cartao_credito
            ,car.nr_expiracao_ano_cartao_credito
            --,pes.id_pessoa as id_responsavel_cartao_credito
            --,pes.nm_pessoa as nm_responsavel_cartao_credito
        from stg_cartoes_creditos car
        left join stg_pessoas_cartoes_creditos pescar on pescar.id_cartao_credito = car.id_cartao_credito
        --left join int_pessoas pes on pes.id_pessoa = carpes.id_pessoa
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_cartao_credito','nr_cartao_credito']) }} as sk_cartao_credito
            ,join_tables.*
        from join_tables
    )

select * from refined 