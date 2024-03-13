package com.data.ceph.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CephAccessRecord extends CephAccessLog implements Serializable {
    public String s3_user_id = "";
    public String display_name = "";
    public String suspended = "";
    public String max_buckets = "";
    public String op_mask = "";
    public String default_placement="";
    public String default_storage_class="";
    public String placement_tags="";
    public String src_ip=""; // 源IP
    public String src_user=""; // 访问用户
    public String country=""; // 源地址 所在国家
    public String city="";//源地址所在城市
    public double request_time=0; // 请求响应时间
    public long body_bytes_sent=0; // 请求响应body大小
    public int status=200; // 请求响应状态
    public String bucket=""; // 桶名
    public String object_id=""; // ObjectID
    public String event_time; //事件时间
    public String event_type=""; //请求类型
    public String request_path=""; //请求路径
    public String dit_ip=""; // 响应机器ip
//    public String user_agent="";    //=============
    public String access_key=""; //
    public String file_type="";//文件类型
    public long file_size= 0; //文件大小
    public int part_num=0; //分片数
    public int part_status;//分片状态  start 0 /ing 1/end 2 /complete 3

    @Override
    public String toString() {
        return "CephAccessRecord{" +
                "s3_user_id='" + s3_user_id + '\'' +
                ", display_name='" + display_name + '\'' +
                ", suspended='" + suspended + '\'' +
                ", max_buckets='" + max_buckets + '\'' +
                ", op_mask='" + op_mask + '\'' +
                ", default_placement='" + default_placement + '\'' +
                ", default_storage_class='" + default_storage_class + '\'' +
                ", placement_tags='" + placement_tags + '\'' +
                ", src_ip='" + src_ip + '\'' +
                ", src_user='" + src_user + '\'' +
                ", country='" + country + '\'' +
                ", city='" + city + '\'' +
                ", request_time=" + request_time +
                ", body_bytes_sent=" + body_bytes_sent +
                ", status=" + status +
                ", bucket='" + bucket + '\'' +
                ", object_id='" + object_id + '\'' +
                ", event_time='" + event_time + '\'' +
                ", event_type='" + event_type + '\'' +
                ", request_path='" + request_path + '\'' +
                ", dit_ip='" + dit_ip + '\'' +
                ", access_key='" + access_key + '\'' +
                ", file_type='" + file_type + '\'' +
                ", file_size=" + file_size +
                ", part_num=" + part_num +
                ", part_status=" + part_status +
                '}';
    }
}
