-- cdmods.ods_roc_db_c0005_reportmetrics_all_s_d roc reportMetrics_all 最近基础配置表
create external table cdmods.ods_roc_db_c0005_reportmetrics_all_s_d(
     rcu_code string comment 'rcu code',
     asr string comment 'asr配置',
     tts string comment 'tts配置',
     face_detect string comment '面部识别配置',
     rcu_camera string comment 'rcu相机配置',
     top_camera string comment 'top相机配置',
     network string comment '网络配置',
     screen_status string comment '屏幕状态',
     operation_mode string comment '操作模式',
     k8s_env_name string comment '环境名称',
     `timestamp` string comment '时间戳'
)
comment 'roc reportMetrics_all最近基础配置表'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;