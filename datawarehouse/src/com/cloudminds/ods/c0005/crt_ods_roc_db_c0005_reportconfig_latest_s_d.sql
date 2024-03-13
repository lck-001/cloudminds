-- cdmods.ods_roc_db_c0005_reportconfig_latest_s_d roc reportConfig_latest最近基础配置表
create external table cdmods.ods_roc_db_c0005_reportconfig_latest_s_d(
     id string comment 'id',
     asr string comment 'asr配置',
     tts string comment 'tts配置',
     face_detect string comment '面部识别配置',
     rcu_camera string comment 'rcu相机配置',
     network string comment '网络配置',
     k8s_env_name string comment '环境名称',
     `timestamp` string comment '时间戳'
)
comment 'roc reportConfig_latest最近基础配置表'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;