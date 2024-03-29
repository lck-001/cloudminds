-- cdmods.ods_crio_db_c0012_user_i_d 货柜用户信息
create external table cdmods.ods_crio_db_c0012_user_i_d(
     id int comment '用户id',
     name string comment '名字',
     username string comment '用户名',
     password string comment '密码',
     token string comment '用户登录token',
     phone string comment '手机号码',
     gender int comment '性别 0：未知 1：男性 2：女性',
     avatar string comment '头像地址',
     wx_unionid string comment '微信联合id',
     wx_openid string comment '微信小程序openid',
     wx_session_key string comment '微信会话密钥',
     wx_contract_id string comment '微信免支付协议id',
     alipay_contract_id string comment '支付宝免支付协议id',
     face_flag int comment '刷脸支付，0-未开通 1-已开通',
     wx_pay_flag int comment '微信免密支付，0-未开通 1-已开通',
     alipay_flag int comment '支付宝免密支付，0-未开通 1-已开通',
     order_paid_total int comment '已付款订单数',
     order_unpay_total int comment '未付款订单数',
     coupon_used_total int comment '已使用红包数',
     coupon_unuse_total int comment '未使用红包数',
     create_time string comment '创建时间',
     update_time string comment '更新时间',
     status int comment '状态：0：冻结 1：正常  9：删除',
     version int comment '乐观锁版本控制',
     alipay_uid string comment '支付宝uid',
     face_id string comment '脸部id',
     dbs_flag int comment 'DBS开通标识：0-未开通，1-已开通',
     dbs_uid string comment 'DBS用户ID',
     dbs_wallet_address string comment 'DBS钱包地址',
     face_image string comment '人脸图片',
     demo_flag int comment 'demo标识：0-正常用户；1-demo用户；',
     mchid string comment '商户号',
     appid string comment '微信appid账号',
     wx_new_openid string comment '新商户的openid',
     only_wx_pay int comment '0：该用户必须使用支付分支付，1：该用户只能使用免密支付',
     wx_pay_score_flag int comment '0：该用户未开通支付分，1：该用户已开通支付分',
     mch_api_key string comment '老商户私钥',
     order_lock string comment '订单锁(订单号)，支付分用户开门时给用户创建支付分订单，使用户开门后不能关闭支付分服务',
     event_time bigint comment '事件时间',
     bigdata_method string comment 'db操作类型:r c u d',
     k8s_env_name string comment '数据来源环境'
)
comment '货柜用户信息crss_cvms.user'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;