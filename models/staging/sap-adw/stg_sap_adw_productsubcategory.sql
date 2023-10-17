with 

source as (

    select * from {{ source('sap_adw', 'productsubcategory') }}

),

renamed as (

    select
        productsubcategoryid
        ,productcategoryid
        ,name
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
