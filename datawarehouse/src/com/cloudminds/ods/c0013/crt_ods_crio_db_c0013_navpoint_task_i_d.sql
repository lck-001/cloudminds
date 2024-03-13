CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0013_navpoint_task_i_d(
    id int COMMENT 'id',
    name string COMMENT '导航点名称',
    pathid int COMMENT '路径任务ID',
    waittime int COMMENT '停留时间',
    posx int COMMENT '坐标',
    posy int COMMENT '坐标',
    fn string COMMENT '导航点功能',
    remarks string COMMENT '备注',
    isdel string COMMENT '是否删除',
    pathname string COMMENT '路径名称',
    robotid string COMMENT '机器id',
    mapname string  COMMENT '地图名称',
    pathchange int COMMENT 'parkingspot表的路径变了不显示',
    event_time BIGINT comment '事件发生时间',
    bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
    k8s_env_name string  comment '环境名称'
) COMMENT 'navpoint_task'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet location '/data/cdmods/ods_crio_db_c0013_navpoint_task_i_d';