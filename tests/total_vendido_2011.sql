with 
    vendas_em_2011 as (
        select round(sum(fct.vl_bruto_item),2) as vl_bruto
        from {{ ref('fct_vendas') }} fct
        left join {{ ref('dim_datas') }} dat on dat.sk_data = fct.sk_data_pedido
        where dat.year_number = 2011
    )
    /*vendas_em_2011 as (
        select round(sum(vl_bruto_item),2) as vl_bruto
        from {{ ref('fct_vendas_obt') }}
        where ano_pedido = 2011
    )*/    

select vl_bruto
from vendas_em_2011
where vl_bruto not between 12646112 and 12646113



