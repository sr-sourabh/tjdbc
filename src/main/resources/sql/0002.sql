select m.*,
       e.*,
       (case when m.startdate > e.startdate then m.startdate else e.startdate end) as finalstartdate,
       (case when m.end < e.end then m.end else e.end end)                         as finalenddate
from manager m
         join employee e on e.did = m.did and
                            ((m.startdate between e.startdate and e.end) or (e.startdate between m.startdate and m.end))
order by e.eid;



tselect
employee tjoin manager on e.did=m.did;

-- manager table

-- manager audit table

create table manager_vt
(
    id        integer AUTO_INCREMENT primary key,
    d_id      int,
    m_id      int,
    startdate date,
    enddate   date
);


insert into manager_vt
values (1, 1, 4, '2001-01-01', '2001-03-31');
insert into manager_vt
values (2, 1, 5, '2001-04-01', '2001-06-30');
insert into manager_vt
values (3, 1, 6, '2001-07-01', '2001-09-30');
insert into manager_vt
values (4, 1, 7, '2001-10-01', '2001-12-31');
insert into manager_vt
values (5, 2, 8, '2001-01-01', '2001-05-31');
insert into manager_vt
values (6, 2, 9, '2001-06-01', '2001-12-31');
insert into manager_vt
values (7, 2, 10, '2001-11-01', '2002-07-31');
insert into manager_vt
values (8, 2, 11, '2002-04-01', '2003-02-28');



-- employee table


-- employee audit table


create table employee_vt
(
    id        integer AUTO_INCREMENT primary key,
    e_id      int,
    d_id      int,
    startdate date,
    enddate   date
);


insert into employee_vt
values (1, 1, 1, '2001-01-01', '2001-05-31');
insert into employee_vt
values (2, 1, 2, '2001-06-01', '2002-05-31');
insert into employee_vt
values (3, 2, 1, '2001-01-01', '2001-07-31');
insert into employee_vt
values (4, 3, 2, '2001-04-01', '2001-09-01');



select m.*,
       e.*,
       (case when m.startdate > e.startdate then m.startdate else e.startdate end) as finalstartdate,
       (case when m.enddate < e.enddate then m.enddate else e.enddate end)         as finalenddate
from manager_vt m
         join employee_vt e on e.d_id = m.d_id and ((m.startdate between e.startdate and e.enddate) or
                                                    (e.startdate between m.startdate and m.enddate))
order by e.e_id;



-- tselect employee tjoin manager on e.did=m.did;


-- employee
create table employee
(
    id   int primary key,
    d_id int,
    name varchar(10),
    lsst timestamp,
    lset timestamp
);
--insertion
insert into employee
values (1, 2,'Ayush' ,'2001-05-31', '2037-01-01');
insert into employee
values (2, 1,'Sourabh' ,'2001-07-31', '2037-01-01');
insert into employee
values (3, 2,'himanshu' ,'2001-09-01', '2037-01-01');

-- employee_vt

CREATE TABLE employee_vt
(
    id            integer AUTO_INCREMENT primary key,
    indx          integer,
    updated_value varchar(10),
    prev_value    varchar(10),
    vst           timestamp,
    vet           timestamp,
    id_id         int
);
-- insertion
insert into employee_vt
values (1, 1, 1, 2,'2001-01-01', '2001-05-31', 1);
insert into employee_vt
values (2, 1, 2, 1,'2001-06-01', '2002-05-31', 1);
insert into employee_vt
values (3, 1, 1, 2,'2001-01-01', '2001-07-31', 2);
insert into employee_vt
values (4, 1, 2, 1,'2001-04-01', '2001-09-01', 3);


-- department

create table department
(
    id   int primary key,
    m_id int,
    name varchar(10),
    lsst timestamp,
    lset timestamp
);


-- Insertion
insert into department
values (1, 7,'CSE' ,'2001-12-31', '2037-01-01');
insert into department
values (2, 11,'ECE' ,'2003-02-28', '2037-01-01');

-- department_vt


CREATE TABLE department_vt
(
    id            integer AUTO_INCREMENT primary key,
    indx          integer,
    updated_value varchar(10),
    prev_value    varchar(10),
    vst           timestamp,
    vet           timestamp,
    id_id         int
);

-- insertion

insert into department_vt
values (1, 1, 4, 3, '2001-01-01', '2001-03-31',1);
insert into department_vt
values (2, 1, 5, 4, '2001-04-01', '2001-06-30',1);
insert into department_vt
values (3, 1, 6, 5, '2001-07-01', '2001-09-30',1);
insert into department_vt
values (4, 1, 7, 6, '2001-10-01', '2001-12-31',1);
insert into department_vt
values (5, 1, 8, 12, '2001-01-01', '2001-05-31',2);
insert into department_vt
values (6, 1, 9, 8, '2001-06-01', '2001-12-31',2);
insert into department_vt
values (7, 1, 10, 9, '2001-11-01', '2002-07-31',2);
insert into department_vt
values (8, 1, 11, 10, '2002-04-01', '2003-02-28',2);




--
--         user query
--         tselect employee e tjoin manager m on e.did = m.did ;
--
--         modified query
--         select m.* , e.* , (case when m.startdate > e.startdate then m.startdate else e.startdate end) as finalstartdate,
--         (case when m.enddate < e.enddate then m.enddate else e.enddate end) as finalenddate from manager_vt m join employee_vt e on e.d_id = m.d_id
--         and ((m.startdate between e.startdate and e.enddate) or (e.startdate between m.startdate and m.enddate)) order by e.e_id;


        select m.* , e.* , (case when m.vst > e.vst then m.vst else e.vst end) as finalstartdate,
        (case when m.vet < e.vet then m.vet else e.vet end) as finalenddate from department_vt m join employee_vt e on e.updated_value = m.id_id
        and ((m.vst between e.vst and e.vet) or (e.vst between m.vst and m.vet));




        select m.* , e.* , (case when m.startdate > e.startdate then m.startdate else e.startdate end) as finalstartdate,
        (case when m.enddate < e.enddate then m.enddate else e.enddate end) as finalenddate from newoldmanager_vt m join newoldemployee_vt e on e.d_id = m.d_id
        and ((m.startdate between e.startdate and e.enddate) or (e.startdate between m.startdate and m.enddate)) order by e.e_id;


select (case when m.vst > e.vst then m.vst else e.vst end) as finalstartdate,
       (case when m.vet < e.vet then m.vet else e.vet end) as finalenddate from department_vt m join employee_vt e on e.updated_value = m.id_id
    and ((m.vst between e.vst and e.vet) or (e.vst between m.vst and m.vet));


select m.id_id as department_id,m.updated_value as m_id, e.id_id as employee_id, e.updated_value as department_id, (case when m.vst > e.vst then m.vst else e.vst end) as finalstartdate,
       (case when m.vet < e.vet then m.vet else e.vet end) as finalenddate from department_vt m join employee_vt e on e.updated_value = m.id_id
    and ((m.vst between e.vst and e.vet) or (e.vst between m.vst and m.vet)) ;
