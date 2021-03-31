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
values (1, 'Mike', 'active', '7.2', 'CSE', '2020-01-01', '2037-01-01');
Insert into student
values (2, 'Shourabh', 'active', '3.8', 'CSE', '2020-02-01', '2037-01-01');
Insert into student
values (3, 'Ayush', 'active', '3.9', 'ECE', '2020-01-22', '2037-01-01');


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

insert into student_vt
values (1, 3, '5.2', '3.3', '2019-01-21', '2019-01-22', 1);
insert into student_vt
values (2, 3, '6.2', '5.2', '2019-01-22', '2019-01-23', 1);
insert into student_vt(indx, updated_value, prev_value, vst, id_id)
values (3, '7.2', '6.2', '2019-01-23', 1);