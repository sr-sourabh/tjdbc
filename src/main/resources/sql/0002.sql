select m.*,
       e.*,
       (case when m.startdate > e.startdate then m.startdate else e.startdate end) as finalstartdate,
       (case when m.end < e.end then m.end else e.end end)                         as finalenddate
from manager m
         join employee e on e.did = m.did and
                            ((m.startdate between e.startdate and e.end) or (e.startdate between m.startdate and m.end))
order by e.eid;



tselect employee tjoin manager on e.did=m.did;

-- manager table

-- manager audit table

create table manager_vt(
                           id integer AUTO_INCREMENT primary key,
                           d_id int,
                           m_id int,
                           startdate date,
                           enddate date );


insert into manager_vt values(1,1,4,'2001-01-01','2001-03-31');
insert into manager_vt values(2,1,5,'2001-04-01','2001-06-30');
insert into manager_vt values(3,1,6,'2001-07-01','2001-09-30');
insert into manager_vt values(4,1,7,'2001-10-01','2001-12-31');
insert into manager_vt values(5,2,8,'2001-01-01','2001-05-31');
insert into manager_vt values(6,2,9,'2001-06-01','2001-12-31');
insert into manager_vt values(7,2,10,'2001-11-01','2002-07-31');
insert into manager_vt values(8,2,11,'2002-04-01','2003-02-28');



-- employee table



-- employee audit table



create table employee_vt(
    id integer AUTO_INCREMENT primary key,
    e_id int,
    d_id int,
    startdate date,
    enddate date );


insert into employee_vt values(1,1,1,'2001-01-01','2001-05-31');
insert into employee_vt values(2,1,2,'2001-06-01','2002-05-31');
insert into employee_vt values(3,2,1,'2001-01-01','2001-07-31');
insert into employee_vt values(4,3,2,'2001-04-01','2001-09-01');






select m.* ,
       e.* ,
       (case when m.startdate > e.startdate then m.startdate else e.startdate end) as finalstartdate,
       (case when m.enddate < e.enddate then m.enddate else e.enddate end)                         as finalenddate
from manager_vt m
         join employee_vt e on e.d_id = m.d_id and
                            ((m.startdate between e.startdate and e.enddate) or (e.startdate between m.startdate and m.enddate))
order by e.e_id;



tselect employee tjoin manager on e.did=m.did;



