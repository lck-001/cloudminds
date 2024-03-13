-----标注04事件--------
---create by liuhao----
CREATE EXTERNAL TABLE cdmods.ods_asr_event_04_i_d(
    json_extract string
    )
PARTITIONED BY (event_id string, dt string)
STORED as textfile
LOCATION '/data/source/04'