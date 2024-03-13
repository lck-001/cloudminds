CREATE EXTERNAL TABLE  cdmods.ods_sv_db_c0003_intent_s_d(
  id bigint comment 'intent id',
  domainid bigint comment 'domain id',
  agentid bigint comment 'agent id',
  intentname string comment 'intent 名称',
  inputcontext string comment '输入上下文',
  outputcontext string comment '输出上下文',
  voicepad string comment '语音垫片',
  prompt string comment '提示词',
  action string COMMENT 'action',
  dfintentid string comment 'dialog flow 意图id',
  luaenabled int COMMENT '是否开启lua脚本 1启用，0关闭',
  reply int COMMENT '是否开启回答 1启用，0关闭',
  k8s_env_name string comment '环境名称'
)COMMENT 'sv库intent表 an agent have many domains,a domain have many intents'
PARTITIONED BY(dt string)
STORED AS PARQUET
location '/data/cdmods/ods_sv_db_c0003_intent_s_d';