/* 
MARKUP:
    1 - Pega o primeiro de customer id que aparecer
    2 - Particona se o company_name e contact_name forem iguais ordenados pelo nome da company_name
    3 - Rows beteween ... significa que a função de janela será aplicada a todas as linhas da janela, 
    desde o início até o fim do conjunto de dados, sem qualquer restrição de limite.
    Suponha que temos uma tabela chamada "Sales" com as colunas "Product" (produto) e "Revenue" (receita), 
    contendo dados de vendas. Queremos calcular a média de receita para cada produto, considerando todas as
    linhas da tabela.
    
    SELECT Product, Revenue, 
    AVG(Revenue) OVER(ORDER BY Product ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS AverageRevenue
    FROM Sales;
REMOVED:
    Vai selecionar somente os ids que foram deduplicados da tabela acima
FINAL
    Vai linkar com a tabela original e trazer somente os ids unicos
*/

-- iniciando CTE ...
with markup as (
    select 
        *,
        first_value(customer_id) 
        over(partition by company_name, contact_name order by company_name
        rows between unbounded preceding and unbounded following) as result
    from {{ source('sources', 'customers') }}
), removed as (
    select distinct
        result
    from markup
), final as (
    select
        *
    from 
        {{ source('sources', 'customers') }}
    where
        customer_id in (select result from removed)
)
select * from final