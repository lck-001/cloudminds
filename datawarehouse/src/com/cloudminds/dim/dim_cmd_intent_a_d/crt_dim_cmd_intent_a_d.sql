CREATE EXTERNAL TABLE  cdmdim.dim_cmd_intent_a_d(
  intent_id bigint comment 'intent id',
  domain_id bigint comment 'domain id',
  agent_id bigint comment 'agent id',
  intent_name string comment 'intent 名称',
  input_context string comment '输入上下文',
  output_context string comment '输出上下文',
  voice_pad string comment '语音垫片',
  prompt string comment '提示词',
  action string COMMENT 'action',
  df_intent_id string comment 'dialog flow 意图id',
  lua_enabled int COMMENT '是否开启lua脚本 1启用，0关闭',
  reply int COMMENT '是否开启回答 1启用，0关闭',
  k8s_env_name string COMMENT '环境名',
  create_time string comment '意图创建时间',
  update_time string comment '意图更新时间'
)
COMMENT 'sv库intent表 an agent have many domains,a domain have many intents'
STORED AS PARQUET
location '/data/cdmdim/dim_cmd_intent_a_d';