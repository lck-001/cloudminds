CREATE EXTERNAL TABLE cdmdim.dim_pmd_rcu_sh_d (
    rcu_id string comment 'rcu id',
    rcu_name string COMMENT 'rcu名字',
    imei string comment 'rcu imei码',
    did string COMMENT 'Decentralized Identifier,去中心化的身份id',
    did_credential_id string COMMENT 'idoe中的did-credential-id',
    model string COMMENT 'rcu model',
    os string COMMENT 'rcu操作系统',
    version_code string comment 'rcu 版本号',
    status int COMMENT 'RCU状态: -1-删除,0-正常,1-停用',
    status_name string comment 'RCU状态: -1-删除,0-正常,1-停用',
    rcu_manufacturer_name string comment 'rcu 制造商',
    cpu string COMMENT 'cpu信息',
    ram string COMMENT 'ram信息',
    rom_capacity string COMMENT 'rom总容量',
    rom_available_capacity string COMMENT 'rom可用空间',
    sd_capacity string COMMENT 'sd卡容量',
    sd_available_capacity string COMMENT 'sd卡可用空间',
    camera string COMMENT '摄像头',
    wifi_mac string COMMENT 'wifi mac地址',
    bluetooth_mac string COMMENT '蓝牙mac地址',
    regist_time string COMMENT '注册时间',
    activate_time string COMMENT '激活时间',
    deactivate_time string COMMENT '取消激活时间',
    description STRING COMMENT '描述信息',
    start_time string COMMENT '拉链信息开始时间',
    end_time string COMMENT '拉链信息结束时间',
    k8s_env_name string COMMENT '环境名',
    create_time STRING COMMENT 'rcu创建时间',
    update_time STRING COMMENT 'rcu更新时间'
)
COMMENT 'RCU信息拉链表'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS PARQUET
location '/data/cdmdim/dim_pmd_rcu_sh_d';