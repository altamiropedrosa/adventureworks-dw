with 
    stg_vendedores as (
        select * from {{ ref('stg_sap_adw_vendedores') }}
    )
    ,stg_vendas_territorios as (
        select * from {{ ref('stg_sap_adw_vendas_territorios') }}
    )
    ,stg_paises as (
        select * from {{ ref('stg_sap_adw_paises') }}
    )    
    /*,int_pessoas_joins as (
        select * from {{ ref('int_pessoas_joins') }}
    )*/


    ,join_tables as (
        select 
            ven.id_vendedor
            /*,pes.cd_tipo_pessoa
            ,pes.nm_pessoa as nm_vendedor
            ,pes.cd_email_promocional
            ,pes.ds_email
            ,pes.ds_contato_adicional
            ,pes.ds_dados_demograficos
            ,pes.nm_tipo_telefone
            ,pes.nr_telefone
            ,pes.nm_tipo_endereco
            ,pes.id_endereco
            ,pes.ds_endereco
            ,pes.nm_cidade
            ,pes.id_estado
            ,pes.cd_estado
            ,pes.nm_estado
            ,pes.nr_cep
            ,pes.cd_pais
            ,pes.nm_pais
            ,pes.ds_grupo_territorio
            ,pes.ds_localizacao_espacial*/
            --Dados de Vendas/Comissão
            ,ven.vl_cota_vendas
            ,ven.vl_bonus
            ,ven.pc_comissao
            ,ven.vl_vendas_ytd --campo calculado?
            ,ven.vl_vendas_ultimo_ano --campo_calculado?
            --Dados de Atuação
            ,ter.cd_pais as cd_pais_atuacao
            ,pai.nm_pais as nm_pais_atuacao
            ,ter.ds_grupo_territorio as ds_grupo_territorio_atuacao
        from stg_vendedores ven
        --left join int_pessoas_joins pes on pes.id_pessoa = ven.id_vendedor
        left join stg_vendas_territorios ter on ter.id_territorio = ven.id_territorio
        left join stg_paises pai on pai.cd_pais = ter.cd_pais
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_vendedor']) }} as sk_vendedor
            ,join_tables.*
        from join_tables
    )

select * from refined

