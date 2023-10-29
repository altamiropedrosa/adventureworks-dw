{% snapshot dim_formas_envio_history %}

    {{
        config(
            target_schema='dbt_apedrosa_history',
            unique_key='sk_forma_envio',
            strategy='timestamp',
            updated_at='dt_modificacao',
            invalidate_hard_deletes=True
        )
    }}

select * from {{ ref('dim_formas_envio') }}

 {% endsnapshot %}