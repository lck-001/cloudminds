create external table cdmods.ods_cv_platform_db_c0017_face_detect_i_d (
     `_id` string comment 'mongo ID',
     robot_id string comment 'robot id',
     gender string comment '性别',
     detect_info string comment '侦测信息',
     faceset_id string comment '脸图id',
     mobile string comment '电话号码',
     source string comment '数据来源',
     face_url string comment '用户录入的人脸url',
     idcard string comment '用户证件号',
     name string comment '用户名称',
     is_mask int comment '0未识别 1戴口罩 2未戴口罩',
     face_id int comment '人脸id',
     tag string comment '标签',
     detect_time bigint comment '事件时间',
     detect_images string comment '侦测到的用户图片url',
     customer_code string comment '商户编码'
)
comment 'mongo cv_platform.face_detect 人脸检测'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;
