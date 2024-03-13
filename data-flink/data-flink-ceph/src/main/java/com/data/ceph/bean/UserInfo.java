package com.data.ceph.bean;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
public class UserInfo implements Serializable {
    public String s3_user_id = "";
    public String display_name = "";
    public String suspended = "";
    public String max_buckets = "";
    public String op_mask = "";
    public String default_placement="";
    public String default_storage_class="";
    public String placement_tags="";
    public String access_key="";

    @Override
    public String toString() {
        return "UserInfo{" +
                "s3_user_id='" + s3_user_id + '\'' +
                ", display_name='" + display_name + '\'' +
                ", suspended='" + suspended + '\'' +
                ", max_buckets='" + max_buckets + '\'' +
                ", op_mask='" + op_mask + '\'' +
                ", default_placement='" + default_placement + '\'' +
                ", default_storage_class='" + default_storage_class + '\'' +
                ", placement_tags='" + placement_tags + '\'' +
                ", access_key='" + access_key + '\'' +
                '}';
    }
}
