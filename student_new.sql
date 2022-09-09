create table student(
student_id int primary key  auto_increment,
student_name varchar(10)
);
create table group_(
group_id int primary key auto_increment,
group_name varchar(6)
);
create table exam(
exam_id int primary key auto_increment,
exam_name varchar(10)
);
create table group_student(
group_student_id int primary key auto_increment,
group_id int,
student_id int,
foreign key (group_id) references group_(group_id),
foreign key (student_id) references student(student_id)
);
create table group_student_exam(
group_student_exam_id int primary key auto_increment,
group_student_id int,
exam_id int,
foreign key (group_student_id) references group_student(group_student_id),
foreign key(exam_id) references exam(exam_id)
);

create table result_(
result_id int primary key auto_increment,
group_student_exam_id int,
foreign key (group_student_exam_id) references group_student_exam(group_student_exam_id),
mark int unsigned not null
);
insert into student(student_name) Values('Иванов'), ('Петров'),('Сидоров');
insert into group_(group_name) values('M-3421'),('В-4567');
insert into exam(exam_name) values('Математика'),('Статистика'),('Физика');
insert into group_student(group_id, student_id) values(2,1),(1,2),(1,3);
insert into group_student_exam(group_student_id, exam_id) values(1,1),(1,3),(2,1),(2,2),(3,2);
insert into result_(group_student_exam_id ,mark) values (1,5),(2,3),(3,4),(4,4),(5,5);

select student_name, group_name, exam_name, mark
from result_
join group_student_exam USING(group_student_exam_id)
join group_student USING(group_student_id)
join group_ USING(group_id)
join student using(student_id)
join exam USING(exam_id)
order by 1;

