DROP TABLE IF EXISTS t1;

create table IF NOT EXISTS t1
(
    id        int unsigned primary key auto_increment comment '主键',
    `name`      varchar(128) not null comment '员工姓名',
    sex       char(1)      not null comment '性别',
    married   boolean      not null comment '婚否',
    education tinyint      not null comment '学历：1大专、2本科、3研究生、4博士、5其他',
    tel       varchar(512) comment '电话号码',
    cert_no   varchar(512) comment '身份证号码',
    email     varchar(200) comment '邮箱',
    address   varchar(200) comment '员工住址',
    mgr_id    int unsigned comment '员工上司ID',
    hiredate  date         not null comment '入职日期',
    termdate  date comment '离职日期',
    `status`  tinyint comment '员工状态：1在职、2休假、3离职、4死亡',
    `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    index idx_status (`status`),
    index idx_mgr_id (mgr_id),
    index idx_cert_no (cert_no)
    ) comment '员工表';

INSERT INTO `t1`( `name`, `sex`, `married`, `education`, `tel`, `cert_no`, `email`, `address`, `mgr_id`, `hiredate`, `termdate`, `status`) VALUES ('MockTest', 'M', 1, 5, '18066666666', '330225199909093256','host@outlook.com', '851 Narborough Rd', 29, '2013-03-11', '2019-08-28', 2);

select * from `t1`;

