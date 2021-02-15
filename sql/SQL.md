# SQL Technologies

## SELECT
```sql
SELECT c.customer_id, MIN(c.customer_name), SUM(s.product_sales)
FROM Customers c
JOIN Sales s
  ON c.customer_id = s.customer_id
WHERE s.year = 2016
GROUP BY c.customer_id
```
```sql
SELECT c.customer_id
     , MIN(c.customer_name)
     , SUM(s.product_sales)
     , p.product_id
FROM Customers c
JOIN Sales s
  ON c.customer_id = s.customer_id
JOIN Products p
  ON s.product_id = p.product_id
WHERE s.year = 2016
  AND p.avg_price > 100.0
GROUP BY c.customer_id, p. product_id
```

```sql
WITH sum_sale AS (
SELECT c.customer_id, SUM(s.product_sales)
FROM Customers c
JOIN Sales s
  ON c.customer_id = s.customer_id
WHERE s.year = 2016
GROUP BY c.customer_id
HAVING SUM(s.product_sales) > 1000.0
)
SELECT c.customer_id
     , MIN(c.customer_name)
     , SUM(s.product_sales)
     , p.product_id
FROM Customers c
JOIN Sales s
  ON c.customer_id = s.customer_id
JOIN Products p
  ON s.product_id = p.product_id
JOIN sum_sale
  ON sum_sale.customer_id = c.customer_id
WHERE s.year = 2016
  AND p.avg_price > 100.0
GROUP BY c.customer_id, p. product_id
```

## Advanced JOIN
```sql
SELECT t1.name, t.avgSalary
FROM (SELECT d.name, AVG(p.SALARY) AS avgSalary
      FROM DEPARTMENT d
      JOIN PROFESSOR p
      ON d.id = p.DEPARTMENT_ID
      GROUP BY d.name
     ) t1
JOIN (SELECT d.name, AVG(p.SALARY) AS avgSalary
      FROM DEPARTMENT d
      JOIN PROFESSOR p
      ON d.id = p.DEPARTMENT_ID
      GROUP BY d.name
      ORDER BY avgSalary DESC
      LIMIT 1
     ) t2
ON t1.avgSalary = t2.avgSalary
```
### Tree Structure in SQL
```sql
SELECT CASE
          WHEN P_ID IS NULL THEN CONCAT(ID, ' ROOT')
          WHEN ID in (SELECT DISTINCT P_ID FROM TREE) THEN CONCAT(ID, ' INNER')
          ELSE CONCAT(ID, ' LEAF')
       END
FROM TREE
```
