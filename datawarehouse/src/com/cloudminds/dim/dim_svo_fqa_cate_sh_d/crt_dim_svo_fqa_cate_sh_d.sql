--  modify by dave.liang at 2021-09-22
create external table cdmdim.dim_svo_fqa_cate_sh_d (
    fqa_cate_id string comment 'ID',
    pid string comment '父id',
    cate_name string comment '分类名字',
    level int comment 'level',
    is_work int comment '是否生效 0:no 1:yes',
    is_work_name string comment '是否生效 0:no 1:yes',
    create_time string comment '添加时间',
    update_time string comment '最后更新时间',
    push_action_name string comment '推送动作 为了推送给kafka add delete update',
    is_need_push int comment '是否需要推送kafka 0:yes 1:no',
    is_del int comment '是否删除 0:no 1:yes',
    last_seq_id string comment 'kafka seq_id',
    is_syn_status int comment '是否同步完成 0:no, 1:yes',
    audit_name string comment '记录操作轨迹',
    sv_msg string comment '记录同步轨迹',
    source string comment '来源',
    k8s_env_name string comment '环境名称',
    start_time string comment '当前分类生效时间',
    end_time string comment '当前分类失效时间',
    cate_name_path string comment '分类cate name path',
    cate_id_path string comment '分类id path'
)
comment 'cms 端通用QA库类别'
partitioned by (dt string comment '分区日期')
stored as parquet;