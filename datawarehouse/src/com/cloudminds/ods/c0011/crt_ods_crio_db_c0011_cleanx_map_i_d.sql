CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0011_cleanx_map_i_d(
     id bigint comment "地图ID",
     cleanx_id bigint comment "所属清洁机器人ID",
     name string comment "名称",
     code string comment "编码",
     `floor` bigint comment "所属楼层",
     width bigint comment "宽",
     height bigint comment "高",
     `size` bigint comment "文件大小(kb)",
     url string comment "地图路径",
     create_time string comment "创建时间",
     update_time string comment "修改时间",
     version bigint comment "乐观锁版本号",
     is_default bigint comment "是否为默认地图 0：不是 1：是",
     resolution float comment "地图像素分辨率",
     adjusting_x float comment "校正参数X",
     adjusting_y float comment "校正参数Y",
     pgm_file_url string comment "所属地图PGM文件存储路径",
     yaml_file_url string comment "所属地图YAML文件存储路径",
     event_time BIGINT comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
) COMMENT '清洁-crss_cxms-cleanx_map'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0011_cleanx_map_i_d/';