create external table cdmods.ods_thd_db_clean_robot_detail_i_d(
	all_tasks string comment '计划任务数',
	taskPer string comment '任务完成率',
	electricity string comment '任务耗电',
	fail_tasks string comment '失败任务数',
	measures string comment '实际清洁面积',
	success_tasks string comment '已完成任务数',
	plan_measures string comment '计划清洁面积',
	project_name string comment '项目名称',
	robot_id string comment '设备id',
	robot_mark string comment '设备名称',
	minutes string comment '任务耗时',
	`date` string comment '日期'
)PARTITIONED BY (dt string comment '日期')
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'STORED AS TEXTFILE
location '/data/cdmods/ods_thd_db_clean_robot_detail_i_d';