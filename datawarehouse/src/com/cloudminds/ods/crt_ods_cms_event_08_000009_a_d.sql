-- 2021-11-22 
-- ods_cms_event_08_000009_a_d表 
-- 目前混合了两种事件（000009：粗标事件, asr标注事件：000011）类型的数据
-- see http://172.16.31.107:8888/cloudminds/dataspec/#/businessevent
create external table cdmods.ods_cms_event_08_000009_a_d(
    raw_message string comment 'cms标注原始日志'
)
stored as textfile
location '/data/source/anno-audio-label-event/';