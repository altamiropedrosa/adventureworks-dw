with 
    stg_clientes as (
        select * from {{ ref('stg_sap_adw_clientes') }}
    )
    ,stg_pessoas as (
        select * from {{ ref('stg_sap_adw_pessoas') }}
    )
    ,stg_territorios as (
        select * from {{ ref('stg_sap_adw_territorios') }}
    )
    ,stg_paises as (
        select * from {{ ref('stg_sap_adw_paises') }}
    )    
    ,stg_lojas as (
        select * from {{ ref('stg_sap_adw_lojas') }}
    )
    ,stg_telefones as (
        select * from {{ ref('stg_sap_adw_pessoas_telefones') }}
    )
    ,stg_tipos_telefones as (
        select * from {{ ref('stg_sap_adw_tipos_telefones') }}
    )
    ,stg_lifetimevalue as (
        select 
            id_cliente
            ,cast(min(dt_pagamento) as datetime) as dt_primeiro_pedido
            ,count(id_pedido_venda) as qt_pedidos
            ,DATETIME_DIFF(current_date(), cast(max(dt_pagamento) as datetime), MONTH) as qt_meses_ultimo_pedido
            ,round(sum(vl_pago),2) as vl_life_time_value
        from {{ ref('stg_sap_adw_pedidos_vendas') }}
        where dt_pagamento is not null
        group by id_cliente
    )

    ,join_tables as (
        select 
            cli.id_cliente
            ,cli.id_pessoa
            ,case when cli.id_pessoa is null then 'Não Identificado' 
                else concat(case when pescli.ds_titulo is null then '' else pescli.ds_titulo end,' ',
                            case when pescli.ds_primeiro_nome is null then '' else pescli.ds_primeiro_nome end,' ',
                            case when pescli.ds_nome_meio is null then '' else pescli.ds_nome_meio end,' ',
                            case when pescli.ds_ultimo_nome is null then '' else pescli.ds_ultimo_nome end) 
            end as nm_cliente
            ,case when pescli.cd_email_promocional = 0 then 'NAO CONTACTAR'
                when pescli.cd_email_promocional = 1 then 'CONTACTAR POR ADW'
                when pescli.cd_email_promocional = 2 then 'CONTACTAR POR ADW + PARCEIROS'
                else null
            end as cd_email_promocional
            ,cli.id_loja
            ,loj.nm_loja
            ,loj.id_vendedor
            ,case when loj.id_vendedor is null then 'Não Identificado' 
                else concat(case when pesven.ds_titulo is null then '' else pesven.ds_titulo end,' ',
                            case when pesven.ds_primeiro_nome is null then '' else pesven.ds_primeiro_nome end,' ',
                            case when pesven.ds_nome_meio is null then '' else pesven.ds_nome_meio end,' ',
                            case when pesven.ds_ultimo_nome is null then '' else pesven.ds_ultimo_nome end) 
            end as nm_vendedor
            ,cli.id_territorio
            ,ter.nm_territorio
            ,ter.ds_grupo_territorio
            ,ter.cd_pais
            ,pai.nm_pais 
            ,case when ltv.qt_pedidos is null then 0 else ltv.qt_pedidos end as qt_pedidos
            ,ltv.qt_meses_ultimo_pedido
            ,case when ltv.vl_life_time_value is null then 0 else ltv.vl_life_time_value end as vl_life_time_value

        from stg_clientes cli
        left join stg_pessoas pescli on pescli.id_pessoa = cli.id_pessoa
        left join stg_territorios ter on ter.id_territorio = cli.id_territorio
        left join stg_paises pai on pai.cd_pais = ter.cd_pais
        left join stg_lojas loj on loj.id_loja = cli.id_loja
        left join stg_pessoas pesven on pesven.id_pessoa = loj.id_vendedor
        left join stg_lifetimevalue ltv on ltv.id_cliente = cli.id_cliente
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_cliente']) }} as sk_cliente
            ,join_tables.*
        from join_tables
    )

select * from refined 