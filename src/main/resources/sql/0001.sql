--as root user execute
create
database tjdbc;

--create a test user
CREATE
USER 'test'@'localhost' IDENTIFIED BY 'test';

--grant priveleges to test user
GRANT ALL PRIVILEGES ON  tjdbc.* TO
'test'@'localhost';

--check if priveleges updated
show
grants for 'test'@'localhost';

--switch to test user then
use
tjdbc;

create table student
(
    id     int primary key,
    name   varchar(10),
    status varchar(10),
    gpa    varchar(10),
    major  varchar(10),
    lsst   timestamp,
    lset   timestamp
);

Insert into student
values (1, 'Mike', 'active', '7.2', 'CSE', '2016-01-01', '2037-01-01');
Insert into student
values (2, 'Shourabh', 'active', '3.8', 'CSE', '2016-02-01', '2037-01-01');
Insert into student
values (3, 'Ayush', 'active', '3.9', 'ECE', '2016-01-22', '2037-01-01');


CREATE TABLE student_vt
(
    id            integer AUTO_INCREMENT primary key,
    indx          integer,
    updated_value varchar(10),
    prev_value    varchar(10),
    vst           timestamp,
    vet           timestamp,
    id_id         int
);

--student 1
--gpa change
insert into student_vt
values (1, 3, '5.2', '3.3', '2019-01-21', '2019-01-22', 1);
insert into student_vt
values (2, 3, '6.2', '5.2', '2019-01-22', '2019-01-23', 1);
insert into student_vt(indx, updated_value, prev_value, vst, id_id)
values (3, '7.2', '6.2', '2019-01-23', 1);
--name change
insert into student_vt(indx, updated_value, prev_value, vst, vet, id_id)
values (1, 'veeru', 'jay', '2019-01-21', '2019-01-22', 1);
insert into student_vt(indx, updated_value, prev_value, vst, vet, id_id)
values (1, 'amit', 'veeru', '2019-01-22', '2019-01-23', 1);
insert into student_vt(indx, updated_value, prev_value, vst, id_id)
values (1, 'Mike', 'amit', '2019-01-23', 1);
--status change
insert into student_vt(indx, updated_value, prev_value, vst, vet, id_id)
values (2, 'inactive', 'processing', '2017-02-21', '2018-03-27', 1);
insert into student_vt(indx, updated_value, prev_value, vst, id_id)
values (2, 'active', 'inactive', '2018-03-27', 1);

--student 2
--gpa change
insert into student_vt(indx, updated_value, prev_value, vst, vet, id_id)
values (3, '6.8', '5.8', '2018-02-21', '2018-06-23', 2);
insert into student_vt(indx, updated_value, prev_value, vst, id_id)
values (3, '3.8', '6.8', '2018-06-23', 2);

--student 3
--major change
insert into student_vt(indx, updated_value, prev_value, vst, vet, id_id)
values (4, 'PHD', 'XYZ', '2017-02-21', '2018-03-27', 3);
insert into student_vt(indx, updated_value, prev_value, vst, vet, id_id)
values (4, 'MS', 'PHD', '2018-03-27', '2019-01-03', 3);
insert into student_vt(indx, updated_value, prev_value, vst, id_id)
values (4, 'ECE', 'MS', '2019-01-03', 3);
--gpa change
insert into student_vt(indx, updated_value, prev_value, vst, vet, id_id)
values (3, '1.9', '6.9', '2017-02-21', '2018-03-27', 3);
insert into student_vt(indx, updated_value, prev_value, vst, vet, id_id)
values (3, '2.9', '1.9', '2018-03-27', '2019-01-03', 3);
insert into student_vt(indx, updated_value, prev_value, vst, id_id)
values (3, '3.9', '2.9', '2019-01-03', 3);


-- Coalesce table data

CREATE TABLE president_vt
(
    name     varchar(20),
    position varchar(20),
    stt      timestamp,
    ett      timestamp
);

-- data insert
insert into president_vt
values ('James Smith', 'President', '1965-01-01', '1969-01-01');
insert into president_vt
values ('James Smith', 'President', '1969-01-01', '1972-01-01');
insert into president_vt
values ('James Smith', 'President', '1974-01-01', '1976-01-01');
insert into president_vt
values ('Michael clarke', 'Vice President', '1972-01-01', '1974-01-01');
insert into president_vt
values ('Michael clarke', 'Vice President', '1974-01-01', '1977-01-01');
insert into president_vt
values ('Barack Obama', 'President', '2004-01-01', '2008-01-01');
insert into president_vt
values ('Barack Obama', 'President', '2008-01-01', '2012-01-01');
insert into president_vt
values ('Joe Biden', 'Vice President', '2008-01-01', '2012-01-01');
insert into president_vt
values ('Joe Biden', 'President', '2020-01-01', '2023-01-01');


-- Update this for Evolution
update student_vt
set updated_value = 5.2
where id = 14;
update student_vt
set prev_value = 5.2
where id = 15;


update student_vt
set updated_value = 2.9
where id = 2;
update student_vt
set prev_value = 2.9
where id = 3;



