select
    "Employee ID" as "employee_id",
    "Store ID" as "store_id",
    "Name" as "full_name",
    "Position" as "job_title"
from {{ ref('employees') }}