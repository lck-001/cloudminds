CREATE EXTERNAL TABLE cdmdim.dim_cmd_hi_a_d(
    hi_id string comment '坐席id',
    hi_account string comment '坐席账户,即login_id',
    tenant_id string comment '隶属租户code',
    hi_name string comment '坐席名称',
    email string comment '坐席邮箱,绑定的邮箱地址',
    phone string comment '联系电话',
    load_type int comment '坐席来源,0-手工录入,1-文件导入,2-ad导入,3-ad同步',
    load_type_name string comment '坐席来源,0-手工录入,1-文件导入,2-ad导入,3-ad同步',
    status int comment '坐席状态: -1-删除,0-停用,1-启用,2-无效',
    status_name string comment '坐席状态: -1-删除,0-停用,1-启用,2-无效',
    operator_id string comment '操作员id',
    hi_group string comment '坐席分组id',
    sync_batch_no string comment '同步批次号',
    k8s_env_name string comment '环境名称',
    create_time string comment '创建时间',
    update_time string comment '更新时间'
)COMMENT '坐席明细表,account与环境确定'
STORED AS parquet
location '/data/cdmdim/dim_cmd_hi_a_d';