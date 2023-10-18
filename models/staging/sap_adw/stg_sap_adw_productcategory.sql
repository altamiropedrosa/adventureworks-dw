with 

source as (

    select * from {{ source('sap_adw', 'productcategory') }}

),

renamed as (

    select
        productcategoryid
        ,name
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
