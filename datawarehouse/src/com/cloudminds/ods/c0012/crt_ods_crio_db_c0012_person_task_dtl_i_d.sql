-- cdmods.ods_crio_db_c0012_person_task_dtl_i_d 货柜任务单元信息
create external table cdmods.ods_crio_db_c0012_person_task_dtl_i_d(
     id int comment '任务单元ID',
     person_task_id int comment '配送id',
     goods_id int comment '商品id',
     goods_name string comment '商品名称',
     bar_code string comment '商品条形码',
     qty int comment '商品数量',
     create_time string comment '创建时间',
     sku_id int comment '库存商品id',
     actual_qty int comment '实际上货或下架数量',
     update_time string comment '修改时间',
     `floor` int comment '层号',
     type int comment '类型：1-上货；2-下架',
     product_time bigint comment '上货时录入的生产日期',
     event_time bigint comment '事件时间',
     bigdata_method string comment 'db操作类型:r c u d',
     k8s_env_name string comment '数据来源环境'
)
comment '货柜任务单元信息crss_cvms.person_task_dtl'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;