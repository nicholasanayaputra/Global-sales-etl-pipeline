select
    "Store ID" as "store_id",
    "Country" as "country",
    "City" as "city",
    "Store Name" as "store_name",
    "Number of Employees" as "number_of_employees",
    "ZIP Code" as "zip_code",
    "Latitude" as "latitude",
    "Longitude" as "longitude"
from {{ ref('stores') }}