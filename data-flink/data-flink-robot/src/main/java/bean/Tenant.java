package bean;

import lombok.Data;

@Data
public class Tenant {
    public int id;       //ID
    public int vop_id;       //关联的虚拟运营商id
    public String name;        //租户名称
    public String code;        //NUL
    public String phone_area_code;     //NUL
    public String phone;        //手机号码
    public String status;       //状态\r\n1：有效\r\n9：删除
    public String create_time;       //创建时间
    public String update_time;       //修改时间
    public int version;      //乐观锁版本号
    public String service_address;        //NUL
    public String contacts;     //NUL
    public String type;      //0长租、1短租、2试用、3售卖、4测试
    public String industry;     //行业
    public String project_name;     //项目名称
    public String district;        //所属地图
    public String description;     //描述信息
    public String email;        //邮箱
    public String phone_code;       //手机号
    public String log;      //log
    public String environment;      //属于哪个环境  test231  prod231
    public String sub_business;        //子业务列表
    public String time_zone;        //时区
    public String time_zone_name;      //NUL
    public Long update_timestamp;      //0
    public String country;      //NUL
    public String province;     //NUL
    public String city;     //NUL
    public String address;      //NUL
    public int customer_id;      //0
    public String salesman;     //销售人员
    public int vpn_user_limit;       //0
    public String vpn_zone_codes;      //NUL
    public String mail;     //邮箱

    public String getCode() {
        return code;
    }
}
