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
    name varchar(10),
    id   int,
    status varchar(10),
    gpa varchar(10),
    major varchar(10),
    LSST timestamp,
    LSET timestamp
);

insert into student
values ("shourabh", 22);

