-----nlp11事件---
---create by tq----
CREATE EXTERNAL TABLE cdmods.ods_nlp_event_11_i_d(
        json_extract string
        )
PARTITIONED BY (event_id string, dt string)
STORED as textfile
LOCATION '/data/source/11'