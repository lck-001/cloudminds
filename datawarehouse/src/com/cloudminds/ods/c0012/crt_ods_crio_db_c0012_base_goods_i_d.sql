create external table cdmods.ods_crio_db_c0012_base_goods_i_d(
    id int comment 'id',
    name string COMMENT '商品名称',
    price int COMMENT '商品售价 单位：分',
    image string COMMENT '商品图片路径',
    bar_code string COMMENT '商品条形码编码',
    out_bar_code string COMMENT '外部商品条形码编码',
    catalog_id int COMMENT '商品分类',
    spec string COMMENT '商品规格',
    weight string COMMENT '商品重量 单位为克',
    brand string COMMENT '品牌名',
    description string COMMENT '商品简介',
    status int COMMENT '状态 0:无效 1:有效 9:删除',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '修改时间',
    version int COMMENT '乐观锁版本号',
    expire_date int COMMENT '商品保质期，单位天'
)COMMENT '货柜-base_goods库'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0012_base_goods_i_d';