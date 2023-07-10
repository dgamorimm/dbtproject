/*
TOTAL:
    Faz a multiplicação do preço unitário + quantitdade = total
DISCOUNT:
    Aplica o disconto aplicando a mesma conta porém com o preço de prateleira, menos o total.
*/

with total as (
    select
    order_id, product_id, unit_price, quantity,
    unit_price * quantity as total
    from {{ source('sources', 'order_details') }}
), discount as (
    select
        t.*,
        pr.product_name, pr.supplier_id, pr.category_id,
        (pr.unit_price * t.quantity) - t.total as discount
    from total as t
    left join
        {{ source('sources', 'products') }} as pr on (t.product_id = pr.product_id)
) 

select * from discount