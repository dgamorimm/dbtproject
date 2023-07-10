/*
PROD:
    tabela fazendo a junção de dados dos fornecedores, produtos e categoria
ORDDETAI:
    tabela fazendo a junção do detalhe das ordens, como preço e desconto com a PROD
ORDRS:
    tabela de ordens que faz junção com os clientes, funcionários e tranportadora
FINALJOIN:
    table que faz a junção entre ORDRS e ORDDETAI
*/

with prod as (
    select
        ct.category_name, 
        sp.company_name as suppliers,
        pd.product_name,
        pd.unit_price,
        pd.product_id
    from 
        {{ source('sources', 'products') }} as pd
    left join
        {{ source('sources', 'suppliers') }} as sp on (pd.supplier_id = sp.supplier_id)
    left join
        {{ source('sources', 'categories') }} as ct on (pd.category_id = ct.category_id)
), orddetai as (
    select
        pd.*,
        od.order_id,
        od.quantity,
        od.discount
    from 
        {{ ref('orderdetails') }} od
    left join
        prod as pd on (od.product_id = pd.product_id) 
), ordrs as (
    select 
        ord.order_date, 
        ord.order_id,
        cs.company_name as customer,
        em.name as employee,
        em.age, 
        em.length_of_service
    from
        {{ source('sources', 'orders') }} ord
    left join
        {{ ref('customers') }} as cs on (ord.customer_id = cs.customer_id)
    left join
        {{ ref('employees') }} as em on (ord.employee_id = em.employee_id)
    left join
        {{ source('sources', 'shippers') }} as sh on (ord.ship_via = sh.shipper_id)
), finaljoin as(
    select
        od.*,
        ord.order_date, ord.customer, ord.employee, ord.age, ord.length_of_service
    from 
        orddetai as od
    inner join
        ordrs as ord on (od.order_id = ord.order_id)
)

select * from finaljoin