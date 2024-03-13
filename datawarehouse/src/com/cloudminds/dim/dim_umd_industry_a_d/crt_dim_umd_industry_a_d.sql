-- cdmdim.dim_umd_industry_a_d 用户行业
create external table cdmdim.dim_umd_industry_a_d(
     industry_id string comment '行业id标识',
     industry_name string comment '行业名称',
     create_time string comment '行业信息创建时间',
     update_time string comment '行业信息更新时间',
     k8s_env_name string comment '环境'
)
comment '用户行业信息'
stored as parquet;