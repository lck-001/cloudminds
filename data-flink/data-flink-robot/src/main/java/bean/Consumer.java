package bean;

import lombok.Data;

@Data
public class Consumer {
    public int id;           //AUTO_INCREMEN
    public String uuid;         //客户令牌
    public String name;         //客户名字
    public String salesman;         //销售人员
    public String create_time;          //创建时间
    public String update_time;          //更新时间
    public String father_id;            //父级id（所属客户编号）
    public String region_code;          //所属区域编号
    public String region_name;          //所属区域名称
    public String nature_code;          //客户性质编号
    public String nature_name;          //客户性质名称
    public String source_code;          //客户来源编号
    public String source_name;          //客户来源名称
    public String category_code;            //客户类别编号
    public String category_name;            //客户类别名称
    public String industry_code;            //客户所属行业编号
    public String industry_name;            //客户所属行业名称
    public String credit_code;          //客户信用编号
    public String credit_name;          //客户信用名称
    public String contact_code;         //客户联系策略编号
    public String contact_name;         //客户联系策略名称
    public String purchase_code;            //客户购买策略编号
    public String purchase_name;            //客户购买策略名称
    public int staff_num;            //员工数量
    public String scale_code;           //客户规模编号
    public String scale_name;           //客户规模名称
    public String employment_code;          //客户从业时间编号
    public String employment_name;          //客户从业时间名称
    public String status_code;          //客户行业地位编号
    public String status_name;          //客户行业地位名称
    public String settlement_method_code;           //客户结算方式编号
    public String settlement_method_name;           //客户结算方式名称
    public String agent;            //希望代理
    public String phone;            //电话
    public String fax;          //传真
    public String mail;         //邮箱
    public String website;          //NUL
    public String address;          //通讯地址
    public String post_code;            //邮编
    public int consumption_num;          //交易次数
    public Double money;            //首次交易金额
    public String first_deal_time;          //首次交易时间
    public String lately_deal_time;         //最近交易时间
    public String customer_tag;         //客户层级
    public Long update_timestamp;         //更新时间戳
    public int start_status;         //启停状态：1：启用，0：停用

    public String getName() {
        return name;
    }
}
