CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0011_cleanx_task_i_d(
     id bigint comment '任务ID',
     cleanx_id bigint comment '所属清洁机器人ID',
     cleanx_map_id bigint comment '所属地图ID',
     name string comment '名称',
     code string comment '编码',
     task_type bigint comment '任务类型 0: 临时任务 1：定时任务',
     work_types string comment '工作类型 0:洗地 1:吸污 2:扫地3:广告4:尘推',
     repeat_times bigint comment '重复次数，0表示无数次',
     days string comment '星期几（启动日期）',
     start_time string comment '任务开始时间',
     end_time string comment '任务结束时间',
     create_time string comment '创建时间',
     update_time string comment '更新时间',
     version bigint comment '乐观锁版本号',
     map_name string comment '所属地图名称',
     map_code string comment '所属地图编码',
     repeat_type bigint comment '循环周期，日循环：1，周循环：7，月循环：30',
     sub_tasks string comment '子任务,json数组',
     event_time BIGINT comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
) COMMENT '清洁-crss_cxms-cleanx_task'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;