create database spark_demo;

use spark_demo;

create table user(
    id int not null auto_increment,
    username varchar(50),
    age int,
    primary key (id)
);

insert into user(username, age) values('xiaofang',18);
insert into user(username, age) values('xiaohua',17);
insert into user(username, age) values('xiaoqiang',19);

select * from user;