-- cdmdwd.dwd_crio_vmd_ceph_event_i_d ceph存储信息
create external table cdmdwd.dwd_crio_vmd_ceph_event_i_d (
    object_id string comment '对象id',
    s3_user_id string comment '用户id',
    display_name string comment '用户名称',
    file_size bigint comment '文件大小',
    file_type string comment '文件类型',
    compress_file_size bigint comment '压缩后文件大小',
    bucket string comment '文件名',
    md5 string comment '数据md5值',
    user_data string comment '用户数据',
    event_time string comment '事件时间',
    suspended string comment 'suspended',
    access_key string comment '用户访问密匙',
    max_buckets string comment '最大文件数',
    placement_tags string comment 'placement stage',
    default_placement string comment 'default placement',
    default_storage_class string comment 'default storage class',
    op_mask string comment '操作标识 read,write,delete',
    k8s_env_name string comment '数据来源环境'
)
comment 'ceph存储信息'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_crio_vmd_ceph_event_i_d';