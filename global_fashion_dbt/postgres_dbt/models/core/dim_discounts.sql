select
    "Start" as "start",
    "End" as "end",
    "Discont" as "discount",
    "Description" as "description",
    "Category" as "category",
    "Sub Category" as "sub_category"
from {{ ref('discounts') }}
