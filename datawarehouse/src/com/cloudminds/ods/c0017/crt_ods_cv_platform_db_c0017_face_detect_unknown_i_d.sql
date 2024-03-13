create external table cdmods.ods_cv_platform_db_c0017_face_detect_unknown_i_d (
     `_id` string comment 'mongo ID',
     robot_id string comment 'robot id',
     detect_info string comment '侦测信息',
     faceset_id string comment '脸图id',
     source string comment '数据来源',
     is_mask int comment '0未识别 1戴口罩 2未戴口罩',
     tag string comment '标签',
     detect_time bigint comment '事件时间',
     detect_images string comment '侦测到的用户图片url',
     customer_code string comment '商户编码'
)
comment 'mongo cv_platform.face_detect_unknown 未检测到人脸'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;