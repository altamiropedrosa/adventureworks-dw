with 
    vendas_em_2011 as (
        select round(sum(vl_subtotal),2) as total_bruto_vendido
        from {{ ref('fct_pedido_vendas') }}
        where cast(dt_pedido as date) between '2011-01-01' and '2011-12-31'
    )


select total_bruto_vendido
from vendas_em_2011
--where total_bruto_vendido not between 658388 and 658389
