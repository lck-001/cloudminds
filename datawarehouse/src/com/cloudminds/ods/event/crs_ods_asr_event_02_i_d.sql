-----标注02事件---
---create by liuhao----
CREATE EXTERNAL TABLE cdmods.ods_rcu_event_02_i_d(
        json_extract string
        )
PARTITIONED BY (event_id string,k8s_env_name string, dt string)
STORED as textfile
LOCATION '/data/source/02'