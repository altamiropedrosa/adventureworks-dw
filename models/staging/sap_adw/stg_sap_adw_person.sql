with 

source as (

    select * from {{ source('sap_adw', 'person') }}

),

renamed as (

    select
        businessentityid
        ,persontype
        ,namestyle
        ,title
        ,firstname
        ,middlename
        ,lastname
        ,suffix
        ,emailpromotion
        ,additionalcontactinfo
        ,demographics
        ,rowguid
        ,modifieddate

    from source

)

select * from renamed
