with 
    vendas_em_2011 as (
        select round(sum(vl_bruto_item),2) as vl_bruto
        from {{ ref('fct_vendas_pedidos') }}
        where ano_pedido = 2011
    )

select vl_bruto
from vendas_em_2011
where vl_bruto not between 12646112 and 12646113



