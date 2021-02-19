--as root user execute
create
database tjdbc;

CREATE
USER 'test'@'localhost' IDENTIFIED BY 'test';

GRANT ALL PRIVILEGES ON  tjdbc.* TO
'test'@'localhost';

--check
show
grants for 'test'@'localhost';

--switch to test user
use
tjdbc;

create table student
(
    name varchar(10),
    id   int
);

insert into student
values ("shourabh", 22);

