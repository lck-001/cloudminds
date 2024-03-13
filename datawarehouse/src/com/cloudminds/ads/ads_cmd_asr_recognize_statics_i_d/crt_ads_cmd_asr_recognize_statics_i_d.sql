-- cdmads.ads_cmd_asr_recognize_statics_i_d asr识别语言次数统计
create external table cdmads.ads_cmd_asr_recognize_statics_i_d (
     tenant_id string comment '租户id',
     tenant_name string comment '租户名称',
     robot_type_id string comment '机器人类型id',
     robot_type_name string comment '机器人类型名称',
     robot_type_inner_name string comment '机器人类型名(研发)',
     asr_vendor string comment 'asr 供应商',
     asr_domain string comment 'asr domain',
     k8s_env_name string comment '环境',
     asr_recognize_cnt_sum_1d int comment 'asr 在当前维度下每天识别次数'
)
comment 'asr识别语言次数统计'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmads/ads_cmd_asr_recognize_statics_i_d';