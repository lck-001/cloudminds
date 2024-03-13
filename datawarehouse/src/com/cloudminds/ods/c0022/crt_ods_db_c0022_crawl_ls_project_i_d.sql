-- 运营管理项目配置表
CREATE EXTERNAL TABLE cdmods.ods_db_c0022_crawl_ls_project_i_d(
    id int comment 'mysql ID',
    created string comment '创建时间',
    updated string comment '更新时间',
    status int comment '运营状态 0-未开始 1-在运营 2-到期关闭 3-到期未关闭 4-已取消',
    name string comment '项目名称',
    priority string comment '项目优先级 p0 p1 p2 p3',
    import_date string comment '项目导入日期',
    start_date string comment '项目开始日期',
    end_date string comment '项目结束日期',
    category string comment '项目类别 长租 短租 售卖 展会演示 客户演示 达闼展厅',
    industry string comment '场景展厅 展会 通用 商业中心/综合体 物业 售楼处 图书馆 手机卖场 移动/电信等营业厅 银行网点 办证中心 酒店 机场 地铁 游客中心/旅游推荐 医院 交警车管所 法院 政务中心 保险营业厅 校园 汽车4S店 博物馆',
    trainer int comment '训练师id',
    trainer_name string comment '训练师名称',
    operator int comment '项目运营者id',
    operator_name string comment '项目运营者名称',
    content_operator int comment '内容运营者id',
    content_operator_name string comment '内容运营者名称',
    agents string comment 'sv_agent_id',
    ts_ms bigint comment '操作时间',
    op string comment '数据库操作类型',
    db string comment '数据库名称',
    `table` string comment '表名称'
) COMMENT '清洁-crss_map-t_map'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/source/sv/bj-prod-232/crawl/ls_project/';