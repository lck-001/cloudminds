-- 项目运营状态表
CREATE EXTERNAL TABLE IF NOT EXISTS cdmdwd.dwd_cms_project_op_i_d(
    event_id string comment '变更操作记录id',
    mid string comment 'mysql ID',
    project_name string comment '项目名称',
    priority string comment '项目优先级 p0 p1 p2 p3',
    import_date string comment '项目导入日期',
    start_date string comment '项目开始日期',
    end_date string comment '项目结束日期',
    category string comment '项目类别 长租 短租 售卖 展会演示 客户演示 达闼展厅',
    scene string comment '场景展厅 展会 通用 商业中心/综合体 物业 售楼处 图书馆 手机卖场 移动/电信等营业厅 银行网点 办证中心 酒店 机场 地铁 游客中心/旅游推荐 医院 交警车管所 法院 政务中心 保险营业厅 校园 汽车4S店 博物馆',
    trainer_id string comment '训练师id',
    trainer_name string comment '训练师名称',
    operator_id string comment '项目运营者id',
    operator_name string comment '项目运营者名称',
    content_operator_id string comment '内容运营者id',
    content_operator_name string comment '内容运营者名称',
    sv_agent_id string comment 'sv_agent_id',
    status int comment '运营状态 0-未开始 1-在运营 2-到期关闭 3-到期未关闭 4-已取消',
    status_name string comment '运营状态名称 0-未开始 1-在运营 2-到期关闭 3-到期未关闭 4-已取消',
    op string comment '数据库操作类型crud',
    created_at string comment '创建时间',
    updated_at string comment '更新时间',
    event_time string   comment '事件发生的时间',
    k8s_env_name string comment '环境名'
) comment '机器人系统音量数据事实表'
PARTITIONED BY ( dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_cms_project_op_i_d'