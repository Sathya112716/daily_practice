# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE company;
# MAGIC USE company;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employees(
# MAGIC     employee_id INT,
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50),
# MAGIC     department_id INT,
# MAGIC     salary DECIMAL(10, 2)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO employees(employee_id, first_name, last_name, department_id, salary) VALUES
# MAGIC (1, 'John', 'Doe', 101, 80000.00),
# MAGIC (2, 'Jane', 'Smith', 102, 85000.00),
# MAGIC (3, 'Alice', 'Johnson', 101, 80000.00),
# MAGIC (4, 'Bob', 'Brown', 103, 55000.00),
# MAGIC (5, 'Charlie', 'Davis', 102, 85000.00),
# MAGIC (6, 'Diana', 'Miller', 101, 90000.00),
# MAGIC (7, 'Edward', 'Wilson', 103, 70000.00),
# MAGIC (8, 'Fiona', 'Clark', 101, 75000.00),
# MAGIC (9, 'George', 'Lee', 102, 60000.00),
# MAGIC (10, 'Hannah', 'Walker', 103, 72000.00);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   department_id,
# MAGIC   salary,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS row_num
# MAGIC FROM
# MAGIC   employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   department_id,
# MAGIC   salary,
# MAGIC   RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rank
# MAGIC FROM
# MAGIC   employees;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   department_id,
# MAGIC   salary,
# MAGIC   DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dense_rank
# MAGIC FROM
# MAGIC   employees;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   salary,
# MAGIC   LAG(salary, 1) OVER ( ORDER BY salary DESC) AS previous_salary
# MAGIC FROM
# MAGIC   employees;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   salary,
# MAGIC   LAG(salary, 1) OVER (PARTITION BY department_id ORDER BY salary DESC) AS previous_salary
# MAGIC FROM
# MAGIC   employees;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   salary,
# MAGIC   LEAD(salary, 1) OVER (ORDER BY employee_id) AS next_salary
# MAGIC FROM
# MAGIC   employees;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   department_id,
# MAGIC   employee_id,
# MAGIC   salary,
# MAGIC   SUM(salary) OVER (PARTITION BY department_id) AS department_total_salary,
# MAGIC   AVG(salary) OVER (PARTITION BY department_id) AS department_avg_salary
# MAGIC FROM
# MAGIC   employees;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT employee_id, first_name, last_name, salary
# MAGIC FROM employees
# MAGIC WHERE salary > (SELECT AVG(salary) FROM employees);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT employee_id, first_name,last_name,salary
# MAGIC FROM employees e1
# MAGIC WHERE salary > (SELECT AVG(salary) 
# MAGIC                 FROM employees e2 
# MAGIC                 WHERE e1.department_id = e2.department_id);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define CTE to get average salary
# MAGIC WITH AvgSalaryCTE AS (
# MAGIC     SELECT AVG(salary) AS avg_salary
# MAGIC     FROM employees
# MAGIC )
# MAGIC -- Query using the CTE to find employees above average salary
# MAGIC SELECT employee_id, first_name, last_name, salary
# MAGIC FROM employees
# MAGIC WHERE salary > (SELECT avg_salary FROM AvgSalaryCTE);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view to get employees with salaries above average
# MAGIC CREATE VIEW EmployeesAboveAvgSalary AS
# MAGIC SELECT employee_id, first_name, last_name, salary
# MAGIC FROM employees
# MAGIC WHERE salary > (SELECT AVG(salary) FROM employees_1);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the view to retrieve employees with salaries above average
# MAGIC SELECT * FROM EmployeesAboveAvgSalary;

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE PROCEDURE procedure_name
# MAGIC  --[ @parameter1 datatype = default_value, ]
# MAGIC  --[ @parameter2 datatype = default_value, ... ]
# MAGIC ---AS
# MAGIC ---BEGIN
# MAGIC  -- SQL statements
# MAGIC --END;
# MAGIC --Syntax for creating a stored procedure
# MAGIC CREATE PROC GetEmployeesByDepartment
# MAGIC     @dept_id INT
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SELECT employee_id, first_name, last_name, salary
# MAGIC     FROM employees
# MAGIC     WHERE department_id = @dept_id;
# MAGIC END;
# MAGIC

# COMMAND ----------

EXEC GetEmployeesByDepartment @dept_id = 101;


# COMMAND ----------


