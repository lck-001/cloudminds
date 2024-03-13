-- cdmdim.dim_pmd_robot_type_dict_a_d 机器人类型字典表
create external table cdmdim.dim_pmd_robot_type_dict_a_d(
     robot_type_id string comment '机器人类型id',
     robot_type_name string comment '机器人类型名，对外',
     robot_type_inner_name string comment '机器人类型名(研发)',
     create_time string comment '创建时间,dict记录中最早的时间',
     update_time string comment '创建时间,dict记录中最晚的时间'
)
comment '机器人类型字典表'
stored as parquet
location '/data/cdmdim/dim_pmd_robot_type_dict_a_d';