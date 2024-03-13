-----标注10事件---
---create by liuhao----
CREATE EXTERNAL TABLE cdmods.ods_harix_event_10_i_d(
        json_extract string
        )
PARTITIONED BY (event_id string, dt string)
STORED as textfile
LOCATION '/data/source/10'