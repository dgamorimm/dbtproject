/*
AGE:
    pega a parte de uma data, no caso o ano atual - pega a parte de uma data no caso o ano de nascimento = idade
LENGTH OF SERVICE
    pega a parte de uma data, no caso o ano atual - pega a parte de uma data no caso o ano de contratação = tempo de serviço
NAME:
    concatenação do nome com o sobrenome
*/
with calc_employees as (
select
    date_part('year', current_date) - date_part('year', birth_date) as age,
    date_part('year', current_date) - date_part('year', hire_date) as length_of_service,
    first_name || ' ' || last_name as name,
    *
from {{ source('sources', 'employees') }}
)

select * from calc_employees