-- cdmdim.dim_pmd_bigdata_dict_a_manual 大数据字典信息,手动维护
create external table cdmdim.dim_pmd_bigdata_dict_a_manual(
     bigdata_dict_id string comment '字典id',
     bigdata_dict_type int comment '字典类型,1:Ginger任务类型 2:货柜任务类型 3:清洁任务类型 4:安保任务类型',
     dict_name string comment '名称',
     dict_value string comment '值',
     create_time string comment '创建时间',
     update_time string comment '更新时间'
)
comment '大数据字典信息'
row format delimited
fields terminated by ','
stored as textfile
location '/data/cdmdim/dim_pmd_bigdata_dict_a_manual';

--数据位置 /data/cdmdim/dim_pmd_bigdata_dict_a_manual/data.txt

