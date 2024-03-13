--  modify by dave.liang at 2021-09-22
create external table cdmdim.dim_svo_user_qa_cate_sh_d (
    user_qa_cate_id string comment 'ID',
    pid string comment '父id',
    cate_name string comment '分类名字',
    level int comment 'level',
    is_work int comment '是否生效 0:no 1:yes',
    is_work_name string comment '是否生效 0:no 1:yes',
    create_time string comment '添加时间',
    update_time string comment '最后更新时间',
    audit_name string comment '记录操作轨迹',
    agent_id string comment 'sv 的 agent id',
    source string comment '来源',
    uuid string comment 'uuid',
    k8s_env_name string comment '环境名称',
    start_time string comment '当前分类生效时间',
    end_time string comment '当前分类失效时间',
    cate_name_path string comment '分类cate name path',
    cate_id_path string comment '分类id path'
)
    comment 'cms 端用户QA库类别'
    partitioned by (dt string comment '分区日期')
    stored as parquet;