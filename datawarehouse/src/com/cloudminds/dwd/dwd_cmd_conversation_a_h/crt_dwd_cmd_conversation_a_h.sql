-- cdmdwd.dwd_cmd_conversation_i_h Smart voice中hari 请求asr识别声音事件
create external table cdmdwd.dwd_cmd_conversation_i_h(
     question_id string comment '请求id',
     robot_id string comment '机器人id',
     asr_agent_id string comment 'asr服务id',
     event_time string comment '事件发生时时间',
     dt string comment 'dt分区字段'
)
comment 'Smart voice中hari 请求asr识别声音事件'
partitioned by(dt string)
stored as parquet
location '/data/cdmdwd/dwd_cmd_conversation_i_h';