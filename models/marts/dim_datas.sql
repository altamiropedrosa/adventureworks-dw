{{
    config(
        materialized = "table"
    )
}}

with 
    
    date_dimension as (
        select * from {{ ref('dates') }}
    )
    ,fiscal_periods as (
        {{ dbt_date.get_fiscal_periods(ref('dates'), year_end_month=1, week_start_day=1, shift_year=1) }}
    )
    ,join_tables as (
        select
            d.*,
            f.fiscal_week_of_year,
            f.fiscal_week_of_period,
            f.fiscal_period_number,
            f.fiscal_quarter_number,
            f.fiscal_period_of_quarter
        from date_dimension d
        left join fiscal_periods f on d.date_day = f.date_day
    )

    ,refined as (
        select 
            {{ dbt_utils.generate_surrogate_key(['date_day']) }} as sk_data
            ,join_tables.*
        from join_tables
    )

select * from refined
