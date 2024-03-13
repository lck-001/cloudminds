CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0021_t_map_i_d(
     id bigint comment "ID",
     tenant_id int comment "租户ID",
     tenant_code string comment "租户编码",
     branch_id int comment "分支机构ID",
     map_code string comment "地图唯一编码",
     name string comment "名称",
     url string comment "url",
     file_key string comment "地图文件Key",
     `floor` string comment "所属楼层",
     height double comment "高",
     width int comment "宽",
     `size` bigint comment "文件大小(kb)",
     create_time string comment "创建时间",
     update_time string comment "修改时间",
     version int comment "乐观锁版本号",
     adjusting_x float comment "校正参数X",
     adjusting_y float comment "校正参数Y",
     resolution float comment "地图像素分辨率",
     map_update_time bigint comment "地图更新时间",
     forbidden_line string comment "虚拟墙信息",
     map_version int comment "地图版本号",
     event_time BIGINT comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
) COMMENT '清洁-crss_map-t_map'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0021_t_map_i_d';