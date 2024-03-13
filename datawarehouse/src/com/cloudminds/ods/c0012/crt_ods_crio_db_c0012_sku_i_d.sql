-- cdmods.ods_crio_db_c0012_sku_i_d 商品单元信息
create external table cdmods.ods_crio_db_c0012_sku_i_d(
     id int comment '单元ID',
     vending_id int comment '货柜ID',
     door_no int comment '柜门编号,从左到右编号1-N',
     `floor` int comment '层号',
     goods_id int comment '商品ID',
     max_amount int comment '满配数量',
     amount int comment '当前库存',
     create_staff_id int comment '创建人id',
     update_staff_id int comment '修改人id',
     create_time string comment '创建时间',
     update_time string comment '修改时间',
     status int comment '状态,0:无效 1:有效 9:删除',
     version int comment '乐观锁版本号',
     product_time bigint comment '生产日期',
     expire_time bigint comment '过期日期',
     event_time bigint comment '事件时间',
     bigdata_method string comment 'db操作类型:r c u d',
     k8s_env_name string comment '数据来源环境'
)
comment '商品单元信息crss_cvms.sku'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;