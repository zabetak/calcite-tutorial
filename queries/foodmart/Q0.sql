select e.full_name, d.department_description
from fm.employee e
join fm.department d on e.department_id = d.department_id 

