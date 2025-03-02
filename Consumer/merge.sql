MERGE INTO ManagedIcebergCatalog.default.employees t
    USING global_temp.source_data s
    ON t.emp_id = s.emp_id
    WHEN MATCHED THEN
        UPDATE SET
            t.employee_name = s.employee_name,
            t.department = s.department,
            t.state = s.state,
            t.salary = s.salary,
            t.age = s.age,
            t.bonus = s.bonus,
            t.ts = s.ts
    WHEN NOT MATCHED THEN
        INSERT (emp_id, employee_name, department, state, salary, age, bonus, ts)
            VALUES (s.emp_id, s.employee_name, s.department, s.state, s.salary, s.age, s.bonus, s.ts)