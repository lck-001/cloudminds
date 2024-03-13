create external table cdmods.ods_crio_db_c0031_face_detect_i_d (
     id string comment 'mongo ID',
     detect_time string comment '事件时间',
     robot_id string comment 'robot id',
     detect_images string comment '侦测到的用户图片url',
     face_url string comment '用户录入的人脸url',
     customer_code string comment '商户编码',
     faceset_id string comment '脸图id',
     face_id string comment '人脸id',
     name string comment '用户名称',
     mobile string comment '电话号码',
     idcard string comment '用户证件号',
     gender string comment '性别',
     detect_info string comment '侦测信息',
     source string comment '数据来源',
     is_mask int comment '0未识别 1戴口罩 2未戴口罩',
     k8s_env_name string comment '环境名称'
)
comment 'mongo cv_platform.face_detect 机器人人脸识别记录'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;