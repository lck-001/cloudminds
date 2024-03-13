CREATE EXTERNAL TABLE  cdmods.ods_sv_db_c0003_domain_s_d(
  id bigint comment 'domain id',
  agentid bigint comment '代理id',
  domainname string comment 'domain 名称',
  domaininfo string comment 'domain 描述信息',
  createtime int comment 'domain 创建时间',
  type string comment 'domain 类型 第三方、自建',
  status string comment '状态 可用、不可用',
  url string comment 'url',
  callservice string COMMENT '调用接口',
  release int comment '是否发行',
  closed_recg tinyint COMMENT '关闭后是否意图识别',
  closed_hit string COMMENT '关闭后命中话术，多句回复用&&隔开',
  keyword tinyint COMMENT '1：代表走关键词服务，此时服务在redis中算是停用状态 其他：不走服务 ',
  domain_switch_hit string COMMENT '??????',
  k8s_env_name string COMMENT '环境名'
)
PARTITIONED BY(dt string)
COMMENT 'sv库domain表 an agent have many domains,a domain have many intents'
STORED AS PARQUET
location '/data/cdmods/ods_sv_db_c0003_domain_s_d';
