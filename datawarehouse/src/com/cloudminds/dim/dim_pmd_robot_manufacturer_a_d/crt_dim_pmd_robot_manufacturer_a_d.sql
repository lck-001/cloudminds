---- cdmdim.dim_pmd_robot_manufacturer_a_d 机器人制造商信息
create external table cdmdim.dim_pmd_robot_manufacturer_a_d(
    robot_manufacturer_id string comment '机器人制造商id标识',
    robot_manufacturer_name string comment '机器人制造商名称',
    created_at string comment '机器人制造商信息创建时间',
    updated_at string comment '机器人制造商信息更新时间',
    k8s_env_name string comment '环境名'
)
comment '机器人制造商信息'
stored as parquet
location '/data/cdmdim/dim_pmd_robot_manufacturer_a_d';