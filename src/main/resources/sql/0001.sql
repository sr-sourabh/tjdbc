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

CREATE TABLE student_vt
(
    id            integer primary key,
    indx          integer,
    updated_value varchar(10),
    prev_value    varchar(10),
    vst           timestamp,
    vet           timestamp,
    id_id         int
);
