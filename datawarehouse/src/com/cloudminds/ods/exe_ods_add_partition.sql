-- load data inpath '/data/cdm/crss/dim_device/${e_dt_var}/*' into table cdmods.ods_crio_db_c0001_t_device_s_h partition(dt='${e_dt_var}');
ALTER TABLE cdmods.ods_hari_db_c0004_t_seats_s_d ADD IF NOT EXISTS partition (dt='${e_dt_var}') LOCATION '/data/cdmods/ods_hari_db_c0004_t_seats_s_d/${e_dt_var}';
ALTER TABLE cdmods.ods_crio_db_c0001_tenant_s_d ADD IF NOT EXISTS partition (dt='${e_dt_var}') LOCATION '/data/cdmods/ods_crio_db_c0001_tenant_s_d/${e_dt_var}';
ALTER TABLE cdmods.ods_roc_db_c0002_t_tenant_s_d ADD IF NOT EXISTS partition (dt='${e_dt_var}') LOCATION '/data/cdmods/ods_roc_db_c0002_t_tenant_s_d/${e_dt_var}';
ALTER TABLE cdmods.ods_roc_db_c0002_t_rcu_s_d ADD IF NOT EXISTS PARTITION(dt='${e_dt_var}') LOCATION '/data/cdmods/ods_roc_db_c0002_t_rcu_s_d/${e_dt_var}';
ALTER TABLE cdmods.ods_roc_db_c0002_t_user_s_d ADD if NOT EXISTS PARTITION(dt='${e_dt_var}') LOCATION '/data/cdmods/ods_roc_db_c0002_t_user_s_d/${e_dt_var}';
ALTER TABLE cdmods.ods_sv_db_c0003_domain_s_d ADD if NOT EXISTS PARTITION(dt = '${e_dt_var}') LOCATION '/data/cdmods/ods_sv_db_c0003_domain_s_d/${e_dt_var}';
alter table cdmods.ods_crio_db_c0001_t_dict_s_d ADD IF NOT EXISTS partition (dt='${e_dt_var}') location '/data/cdmods/ods_crio_db_c0001_t_dict_a_d/${e_dt_var}';
alter table cdmods.ods_crio_db_c0001_t_customer_s_d ADD IF NOT EXISTS partition (dt='${e_dt_var}') location '/data/cdmods/ods_crio_db_c0001_t_customer_a_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0002_t_environment_s_d ADD IF NOT EXISTS partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0002_t_environment_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0002_t_library_s_d ADD IF NOT EXISTS partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0002_t_library_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0002_t_library_env_i_d ADD IF NOT EXISTS partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0002_t_library_env_i_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0002_t_current_robot_config_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0002_t_current_robot_config_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0002_t_user_library_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0002_t_user_library_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0002_t_user_rcu_robot_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0002_t_user_rcu_robot_s_d/${e_dt_var}';
alter table cdmods.ods_hari_db_c0004_t_seats_service_code_i_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_hari_db_c0004_t_seats_service_code_i_d/${e_dt_var}';
alter table cdmods.ods_hari_db_c0004_t_login_seats_i_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_hari_db_c0004_t_login_seats_i_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0002_t_tenant_library_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0002_t_tenant_library_s_d/${e_dt_var}';
alter table cdmods.ods_hari_db_c0004_t_login_seats_i_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_hari_db_c0004_t_login_seats_i_d/${e_dt_var}';
alter table cdmods.ods_cloud_db_c0007_asset_asset_a_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_cloud_db_c0007_asset_asset_a_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0002_t_robot_supplement_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0002_t_robot_supplement_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0002_t_tenant_robot_type_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0002_t_tenant_robot_type_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0008_t_tenant_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0008_t_tenant_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0008_t_library_s_d add IF NOT EXISTS partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0008_t_library_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0008_t_library_env_i_d add IF NOT EXISTS partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0008_t_library_env_i_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0008_t_environment_s_d add IF NOT EXISTS partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0008_t_environment_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0008_t_rcu_s_d add if not exists partition(dt = "${e_dt_var}") location "/data/cdmods/ods_roc_db_c0008_t_rcu_s_d/${e_dt_var}";
alter table cdmods.ods_roc_db_c0008_t_user_s_d add if not exists partition(dt = "${e_dt_var}") location "/data/cdmods/ods_roc_db_c0008_t_user_s_d/${e_dt_var}";
ALTER TABLE cdmods.ods_crio_db_c0001_t_rcu_s_d ADD if not exists PARTITION(dt="${e_dt_var}") LOCATION "/data/cdmods/ods_crio_db_c0001_t_rcu_s_d/${e_dt_var}";
alter table cdmods.ods_roc_db_c0005_reportmetrics_all_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0005_reportmetrics_all_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0008_t_user_rcu_robot_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0008_t_user_rcu_robot_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0008_t_user_library_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0008_t_user_library_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0008_t_tenant_library_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0008_t_tenant_library_s_d/${e_dt_var}';
alter table cdmods.ods_roc_db_c0008_t_current_robot_config_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_roc_db_c0008_t_current_robot_config_s_d/${e_dt_var}';
alter table cdmods.ods_hari_db_c0009_t_seats_s_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_hari_db_c0009_t_seats_s_d/${e_dt_var}';
alter table cdmods.ods_hari_db_c0009_t_seats_service_code_i_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_hari_db_c0009_t_seats_service_code_i_d/${e_dt_var}';
alter table cdmods.ods_hari_db_c0009_t_login_seats_i_d add if not exists partition (dt='${e_dt_var}')
location '/data/cdmods/ods_hari_db_c0009_t_login_seats_i_d/${e_dt_var}';