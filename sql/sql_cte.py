# Databricks notebook source
CREATE TABLE employees (
    employee_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department_id INT,
    salary DECIMAL(10, 2)
);

INSERT INTO employees (employee_id, first_name, last_name, department_id, salary) VALUES
(1, 'Sathya', 'Priya', 1, 60000),
(2, 'Ravi', 'Chandran', 2, 70000),
(3, 'Lalitha','Karigalan' ', 1, 80000),
(4, 'Eshwar', 'Kumar', 3, 90000),
(5, 'Sanjana', 'sri', 2, 75000);


# COMMAND ----------

#Define the CTE to calculate the average salary per department
WITH AvgSalaryPerDept AS (
    SELECT
    department_id,
    AVG(salary) AS avg_salary
    FROM
        employees
    GROUP BY
        department_id
)
 -- Use the CTE in a SELECT statement
SELECT
    e.employee_id,
    e.first_name,
    e.last_name,
    e.department_id,
    e.salary,
    a.avg_salary
FROM
    employees e
JOIN
    AvgSalaryPerDept a
ON
    e.department_id = a.department_id
WHERE
    e.salary > a.avg_salary;

