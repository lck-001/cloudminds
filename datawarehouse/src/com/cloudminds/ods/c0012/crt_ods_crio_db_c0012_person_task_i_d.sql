-- cdmods.ods_crio_db_c0012_person_task_i_d 货柜任务信息
create external table cdmods.ods_crio_db_c0012_person_task_i_d(
     id int comment '任务ID',
     uuid string comment '任务单号',
     parent_id int comment '父任务ID',
     tenant_id int comment '租户ID',
     tenant_code string comment '租户编码',
     task_type int comment '任务类型：0-配送 ；1-上货；2-下架',
     task_way int comment '任务方式 0-规则上货 1-自由上货',
     name string comment '任务名称',
     person_id int comment '人员ID',
     person_name string comment '人员姓名',
     phone string comment '人员电话',
     vending_id int comment '货柜ID',
     vending_name string comment '货柜名称',
     vending_code string comment '货柜编码',
     address string comment '货柜地址',
     region_id int comment '配送区域',
     start_time string comment '开始时间',
     end_time string comment '结束时间',
     finish_time string comment '完成时间',
     description string comment '描述信息',
     vending_log_id bigint comment '事件ID',
     state int comment '任务状态：\r\n0-等待处理；\r\n1-处理中；\r\n2-已完成；\r\n3-已过期；\r\n4-已领取；\r\n5-待取货；\r\n6-待上货；',
     create_time string comment '创建时间',
     update_time string comment '修改时间',
     status int comment '状态：0-无效；1-有效；9-删除',
     version int comment '乐观锁版本控制',
     sort_out_person_id int comment '分拣人员ID',
     sort_out_time bigint comment '分拣时间',
     event_time bigint comment '事件时间',
     bigdata_method string comment 'db操作类型:r c u d',
     k8s_env_name string comment '数据来源环境'
)
comment '货柜任务信息crss_cvms.person_task'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;