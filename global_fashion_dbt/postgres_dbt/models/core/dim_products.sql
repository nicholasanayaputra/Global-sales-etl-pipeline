select
    cast("Product ID" as text) as product_id,
    "Category" as "category",
    "Sub Category" as "sub_category",
    "Description PT" as "description_pt",
    "Description DE" as "description_de",
    "Description FR" as "description_fr",
    "Description ES" as "description_es",
    "Description EN" as "description_en",
    "Description ZH" as "description_zh",
    "Color" as "color",
    "Sizes" as "size",
    "Production Cost" as "production_cost"
from {{ ref('products') }}