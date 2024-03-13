package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;

import java.text.SimpleDateFormat;

@Data
@AllArgsConstructor
@Getter
public class DeviceWide {
    public String id;   //'RCU_ID'
    public String tenant_code;  //'隶属租户code'
    public String user_id;  //'用户ID'
    public String rcu_id;   //'RcuID'
    public String robot_id; //'RobotID'
    public String user_code;    //'用户登录ID'
    public String rcu_code; //'RCU唯一标识rcuId值'
    public String robot_code;   //'robot唯一标识值'
    public String token;    //'token值'
    public int status;  //'关联信息状态: -1-删除; 0-注册未激活; 1-激活可用; 2-通知VPN注册'

    public String create_time;  //'创建时间'
    public String update_time;  //'修改时间'


    public String asset_code;//  资产编码
    public String product_type_code;//  类型编码
    public String product_type_code_name;//  DEFAULTNULL
    public String supplier_code;//  供应商编码
    public String supplier_code_name;//  供应商code
    public int product_id;//  产品ID
    public String product_id_name;//  产品名字
    public String device_code;//  DEFAULTNULL
    public String device_name;//  DEFAULTNULL
    public String device_model;//  DEFAULTNULL
    public String software_version;//  DEFAULTNULL
    public String hardware_version;//  DEFAULTNULL
    public Long quality_date;//  供应商质保期
    public Long customer_quality_date;//  timestampNULLDEFAULTNULL
    public int device_status;//  1正常9删除
    public Long product_date;//  生产日期
    public String environment;//  属于哪个环境test231prod231
    public String sku;//  DEFAULTNULL
    public String asset_type;//  资产类型001达闼固资002达闼存货003客户资产
    public int roc_delivery_status;//  ROC交付状态0已交付1未交付2交付中3回收中
    public String is_special;//  1常规资产0特殊资产
    public String serial_number;//  序列号
    public int operating_status;//  运营状态1空闲2测试中3演示中4运营中5交付中
    public int running_status;//  运行状态1良好2故障3维修中
    public int asset_status;//  资产状态1在册2待测3研发测试4空闲5项目中
    public int order_overdue;//  订单超时状态
    public String end_time;//  结束时间
    public Long update_timestamp;//  DEFAULTNULL
    public String running_tag;//  运营标签
    public String project_tag;//  项目标签
    public int order_type;//  订单类型
    public String country;//  国家
    public String province;//  省
    public String city;//  市
    public String district;//  区
    public String service_address;//  服务地址
    public String contacts;//  联系人
    public String phone;//  联系电话
    public Double longitude;//  经度
    public Double latitude;//  纬度
    public int if_update_report_location;//  是否更新上报经纬度1更细2不更新
    public String product_category_code;//  产品分类
    public String product_category_name;//  产品分类名称

    public int vop_id;       //关联的虚拟运营商id
    public String name;        //租户名称
    public String code;        //NUL
    public String phone_area_code;     //NUL
    public String tenant_status;       //状态\r\n1：有效\r\n9：删除
    public int version;      //乐观锁版本号
    public String type;      //0长租、1短租、2试用、3售卖、4测试
    public String industry;     //行业
    public String project_name;     //项目名称
    public String description;     //描述信息
    public String email;        //邮箱
    public String phone_code;       //手机号
    public String log;      //log
    public String sub_business;        //子业务列表
    public String time_zone;        //时区
    public String time_zone_name;      //NUL
    public String address;      //NUL
    public int customer_id;      //0
    public String salesman;     //销售人员
    public int vpn_user_limit;       //0
    public String vpn_zone_codes;      //NUL
    public String mail;     //邮箱


    public String uuid;         //客户令牌
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
    public String fax;          //传真
    public String website;          //NUL
    public String post_code;            //邮编
    public int consumption_num;          //交易次数
    public Double money;            //首次交易金额
    public String first_deal_time;          //首次交易时间
    public String lately_deal_time;         //最近交易时间
    public String customer_tag;         //客户层级
    public int start_status;         //启停状态：1：启用，0：停用

    public String rcuservice_versionname;
    public int rcuservice_versioncode;
    public String robot_model;
    public String robot_manufacturer;
    public String robot_robotid;
    public String robot_serialnumber;
    public String robot_softversion;
    public String robot_hardwareversion;
    public String robot_productiondate;
    public String robot_version;
    public String robot_faceboard;
    public String robot_x86;
    public String robot_gaussian;
    public String robot_mcu;
    public String sca_version;
    public String mcsclient_versionname;
    public int mcsclient_versioncode;
    public String mcsclient_simiccid;
    public String micarray_versionname;
    public String rcu_os;
    public String rcu_model;
    public String rcu_manufacturer;
    public String rcu_imei;
    public String ue4Client_versionName;
    public int ue4Client_versionCode;
    public String ECU_version;
    public String rcuApp_versionName;
    public int rcuApp_versionCode;
    public String robotApp_versionName;
    public int robotApp_versionCode;
    public String cloudPepperApp_versionName;
    public int cloudPepperApp_versionCode;
    public String vendingApp_versionName;
    public int vendingApp_versionCode;
    public String digitalBox_versionName;
    public int digitalBox_versionCode;
    public String slam_version;
    public String pad_version;
    public String ikooClient_versionName;
    public int ikooClientversionCode;

    public String library_id;       //知识库信息ID
    public String library_type;     //库类型;
    public int library_from;        //来源：1-客户; 2-分组; 3-账号; 4-系统

    public String library_name;        //'库名称'
    public String library_value;        //库属性信息(json)
    public int library_status;      //知识库状态: -1-删除;0-正常


    public DeviceWide(Relationship relationship, Device device) {
        mergeDevice(device);
        mergeRelationship(relationship);
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String datetime = tempDate.format(new java.util.Date());

        if (StringUtils.isBlank(create_time) == true) {
            this.create_time = datetime;
        } else {
            this.update_time = datetime;
        }
    }

    public DeviceWide(DeviceWide deviceWide, Device device) {
        mergeDevice(device);
        mergeOther(deviceWide);
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String datetime = tempDate.format(new java.util.Date());

        if (StringUtils.isBlank(create_time) == true) {
            this.create_time = datetime;
        } else {
            this.update_time = datetime;
        }
    }

    public DeviceWide(DeviceWide deviceWide, UserLibrary userLibrary) {
        mergeUserLibrary(userLibrary);
        mergeOther(deviceWide);
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String datetime = tempDate.format(new java.util.Date());

        if (StringUtils.isBlank(create_time) == true) {
            this.create_time = datetime;
        } else {
            this.update_time = datetime;
        }
    }


    public DeviceWide(DeviceWide deviceWide, Library library) {
        mergeLibrary(library);
        mergeOther(deviceWide);
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String datetime = tempDate.format(new java.util.Date());

        if (StringUtils.isBlank(create_time) == true) {
            this.create_time = datetime;
        } else {
            this.update_time = datetime;
        }
    }

    public DeviceWide(DeviceWide deviceWide, Consumer consumer) {
        mergeConsumer(consumer);
        mergeOther(deviceWide);
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String datetime = tempDate.format(new java.util.Date());

        if (StringUtils.isBlank(create_time) == true) {
            this.create_time = datetime;
        } else {
            this.update_time = datetime;
        }
    }

    public DeviceWide(Relationship relationship, Tenant tenant) {
        mergeTenant(tenant);
        mergeRelationship(relationship);
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String datetime = tempDate.format(new java.util.Date());

        if (StringUtils.isBlank(create_time) == true) {
            this.create_time = datetime;
        } else {
            this.update_time = datetime;
        }
    }

    public DeviceWide(Relationship relationship, ReportAttribute reportAttribute) {
        mergeReportAttribute(reportAttribute);
        mergeRelationship(relationship);
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String datetime = tempDate.format(new java.util.Date());

        if (StringUtils.isBlank(create_time) == true) {
            this.create_time = datetime;
        } else {
            this.update_time = datetime;
        }
    }

    public DeviceWide(DeviceWide otherWide, Tenant tenant) {
        mergeOther(otherWide);
        mergeTenant(tenant);
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String datetime = tempDate.format(new java.util.Date());

        if (StringUtils.isBlank(create_time) == true) {
            this.create_time = datetime;
        } else {
            this.update_time = datetime;
        }
    }

    public void mergeOther(DeviceWide otherWide) {
        if (otherWide != null) {
            this.id = ObjectUtils.firstNonNull(this.id, otherWide.id);
            this.tenant_code = ObjectUtils.firstNonNull(this.tenant_code, otherWide.tenant_code);
            this.user_id = ObjectUtils.firstNonNull(this.user_id, otherWide.user_id);
            this.rcu_id = ObjectUtils.firstNonNull(this.rcu_id, otherWide.rcu_id);
            this.robot_id = ObjectUtils.firstNonNull(this.robot_id, otherWide.robot_id);
            this.user_code = ObjectUtils.firstNonNull(this.user_code, otherWide.user_code);
            this.rcu_code = ObjectUtils.firstNonNull(this.rcu_code, otherWide.rcu_code);
            this.robot_code = ObjectUtils.firstNonNull(this.robot_code, otherWide.robot_code);
            this.token = ObjectUtils.firstNonNull(this.token, otherWide.token);
            this.status = ObjectUtils.firstNonNull(this.status, otherWide.status);
            this.create_time = ObjectUtils.firstNonNull(this.create_time, otherWide.create_time);
            this.update_time = ObjectUtils.firstNonNull(this.update_time, otherWide.update_time);
            this.asset_code = ObjectUtils.firstNonNull(this.asset_code, otherWide.asset_code);
            this.product_type_code = ObjectUtils.firstNonNull(this.product_type_code, otherWide.product_type_code);
            this.product_type_code_name = ObjectUtils.firstNonNull(this.product_type_code_name, otherWide.product_type_code_name);
            this.supplier_code = ObjectUtils.firstNonNull(this.supplier_code, otherWide.supplier_code);
            this.supplier_code_name = ObjectUtils.firstNonNull(this.supplier_code_name, otherWide.supplier_code_name);
            this.product_id = ObjectUtils.firstNonNull(this.product_id, otherWide.product_id);
            this.product_id_name = ObjectUtils.firstNonNull(this.product_id_name, otherWide.product_id_name);
            this.device_code = ObjectUtils.firstNonNull(this.device_code, otherWide.device_code);
            this.device_name = ObjectUtils.firstNonNull(this.device_name, otherWide.device_name);
            this.device_model = ObjectUtils.firstNonNull(this.device_model, otherWide.device_model);
            this.software_version = ObjectUtils.firstNonNull(this.software_version, otherWide.software_version);
            this.hardware_version = ObjectUtils.firstNonNull(this.hardware_version, otherWide.hardware_version);
            this.quality_date = ObjectUtils.firstNonNull(this.quality_date, otherWide.quality_date);
            this.customer_quality_date = ObjectUtils.firstNonNull(this.customer_quality_date, otherWide.customer_quality_date);
            this.device_status = ObjectUtils.firstNonNull(this.device_status, otherWide.device_status);
            this.product_date = ObjectUtils.firstNonNull(this.product_date, otherWide.product_date);
            this.environment = ObjectUtils.firstNonNull(this.environment, otherWide.environment);
            this.sku = ObjectUtils.firstNonNull(this.sku, otherWide.sku);
            this.asset_type = ObjectUtils.firstNonNull(this.asset_type, otherWide.asset_type);
            this.roc_delivery_status = ObjectUtils.firstNonNull(this.roc_delivery_status, otherWide.roc_delivery_status);
            this.is_special = ObjectUtils.firstNonNull(this.is_special, otherWide.is_special);
            this.serial_number = ObjectUtils.firstNonNull(this.serial_number, otherWide.serial_number);
            this.operating_status = ObjectUtils.firstNonNull(this.operating_status, otherWide.operating_status);
            this.running_status = ObjectUtils.firstNonNull(this.running_status, otherWide.running_status);
            this.asset_status = ObjectUtils.firstNonNull(this.asset_status, otherWide.asset_status);
            this.order_overdue = ObjectUtils.firstNonNull(this.order_overdue, otherWide.order_overdue);
            this.end_time = ObjectUtils.firstNonNull(this.end_time, otherWide.end_time);
            this.update_timestamp = ObjectUtils.firstNonNull(this.update_timestamp, otherWide.update_timestamp);
            this.running_tag = ObjectUtils.firstNonNull(this.running_tag, otherWide.running_tag);
            this.project_tag = ObjectUtils.firstNonNull(this.project_tag, otherWide.project_tag);
            this.order_type = ObjectUtils.firstNonNull(this.order_type, otherWide.order_type);
            this.country = ObjectUtils.firstNonNull(this.country, otherWide.country);
            this.province = ObjectUtils.firstNonNull(this.province, otherWide.province);
            this.city = ObjectUtils.firstNonNull(this.city, otherWide.city);
            this.district = ObjectUtils.firstNonNull(this.district, otherWide.district);
            this.service_address = ObjectUtils.firstNonNull(this.service_address, otherWide.service_address);
            this.contacts = ObjectUtils.firstNonNull(this.contacts, otherWide.contacts);
            this.phone = ObjectUtils.firstNonNull(this.phone, otherWide.phone);
            this.longitude = ObjectUtils.firstNonNull(this.longitude, otherWide.longitude);
            this.latitude = ObjectUtils.firstNonNull(this.latitude, otherWide.latitude);
            this.if_update_report_location = ObjectUtils.firstNonNull(this.if_update_report_location, otherWide.if_update_report_location);
            this.product_category_code = ObjectUtils.firstNonNull(this.product_category_code, otherWide.product_category_code);
            this.product_category_name = ObjectUtils.firstNonNull(this.product_category_name, otherWide.product_category_name);
            this.vop_id = ObjectUtils.firstNonNull(this.vop_id, otherWide.vop_id);
            this.name = ObjectUtils.firstNonNull(this.name, otherWide.name);
            this.code = ObjectUtils.firstNonNull(this.code, otherWide.code);
            this.phone_area_code = ObjectUtils.firstNonNull(this.phone_area_code, otherWide.phone_area_code);
            this.tenant_status = ObjectUtils.firstNonNull(this.tenant_status, otherWide.tenant_status);
            this.version = ObjectUtils.firstNonNull(this.version, otherWide.version);
            this.type = ObjectUtils.firstNonNull(this.type, otherWide.type);
            this.industry = ObjectUtils.firstNonNull(this.industry, otherWide.industry);
            this.project_name = ObjectUtils.firstNonNull(this.project_name, otherWide.project_name);
            this.description = ObjectUtils.firstNonNull(this.description, otherWide.description);
            this.email = ObjectUtils.firstNonNull(this.email, otherWide.email);
            this.phone_code = ObjectUtils.firstNonNull(this.phone_code, otherWide.phone_code);
            this.log = ObjectUtils.firstNonNull(this.log, otherWide.log);
            this.sub_business = ObjectUtils.firstNonNull(this.sub_business, otherWide.sub_business);
            this.time_zone = ObjectUtils.firstNonNull(this.time_zone, otherWide.time_zone);
            this.time_zone_name = ObjectUtils.firstNonNull(this.time_zone_name, otherWide.time_zone_name);
            this.address = ObjectUtils.firstNonNull(this.address, otherWide.address);
            this.customer_id = ObjectUtils.firstNonNull(this.customer_id, otherWide.customer_id);
            this.salesman = ObjectUtils.firstNonNull(this.salesman, otherWide.salesman);
            this.vpn_user_limit = ObjectUtils.firstNonNull(this.vpn_user_limit, otherWide.vpn_user_limit);
            this.vpn_zone_codes = ObjectUtils.firstNonNull(this.vpn_zone_codes, otherWide.vpn_zone_codes);
            this.mail = ObjectUtils.firstNonNull(this.mail, otherWide.mail);
            this.uuid = ObjectUtils.firstNonNull(this.uuid, otherWide.uuid);
            this.father_id = ObjectUtils.firstNonNull(this.father_id, otherWide.father_id);
            this.region_code = ObjectUtils.firstNonNull(this.region_code, otherWide.region_code);
            this.region_name = ObjectUtils.firstNonNull(this.region_name, otherWide.region_name);
            this.nature_code = ObjectUtils.firstNonNull(this.nature_code, otherWide.nature_code);
            this.nature_name = ObjectUtils.firstNonNull(this.nature_name, otherWide.nature_name);
            this.source_code = ObjectUtils.firstNonNull(this.source_code, otherWide.source_code);
            this.source_name = ObjectUtils.firstNonNull(this.source_name, otherWide.source_name);
            this.category_code = ObjectUtils.firstNonNull(this.category_code, otherWide.category_code);
            this.category_name = ObjectUtils.firstNonNull(this.category_name, otherWide.category_name);
            this.industry_code = ObjectUtils.firstNonNull(this.industry_code, otherWide.industry_code);
            this.industry_name = ObjectUtils.firstNonNull(this.industry_name, otherWide.industry_name);
            this.credit_code = ObjectUtils.firstNonNull(this.credit_code, otherWide.credit_code);
            this.credit_name = ObjectUtils.firstNonNull(this.credit_name, otherWide.credit_name);
            this.contact_code = ObjectUtils.firstNonNull(this.contact_code, otherWide.contact_code);
            this.contact_name = ObjectUtils.firstNonNull(this.contact_name, otherWide.contact_name);
            this.purchase_code = ObjectUtils.firstNonNull(this.purchase_code, otherWide.purchase_code);
            this.purchase_name = ObjectUtils.firstNonNull(this.purchase_name, otherWide.purchase_name);
            this.staff_num = ObjectUtils.firstNonNull(this.staff_num, otherWide.staff_num);
            this.scale_code = ObjectUtils.firstNonNull(this.scale_code, otherWide.scale_code);
            this.scale_name = ObjectUtils.firstNonNull(this.scale_name, otherWide.scale_name);
            this.employment_code = ObjectUtils.firstNonNull(this.employment_code, otherWide.employment_code);
            this.employment_name = ObjectUtils.firstNonNull(this.employment_name, otherWide.employment_name);
            this.status_code = ObjectUtils.firstNonNull(this.status_code, otherWide.status_code);
            this.status_name = ObjectUtils.firstNonNull(this.status_name, otherWide.status_name);
            this.settlement_method_code = ObjectUtils.firstNonNull(this.settlement_method_code, otherWide.settlement_method_code);
            this.settlement_method_name = ObjectUtils.firstNonNull(this.settlement_method_name, otherWide.settlement_method_name);
            this.agent = ObjectUtils.firstNonNull(this.agent, otherWide.agent);
            this.fax = ObjectUtils.firstNonNull(this.fax, otherWide.fax);
            this.website = ObjectUtils.firstNonNull(this.website, otherWide.website);
            this.post_code = ObjectUtils.firstNonNull(this.post_code, otherWide.post_code);
            this.consumption_num = ObjectUtils.firstNonNull(this.consumption_num, otherWide.consumption_num);
            this.money = ObjectUtils.firstNonNull(this.money, otherWide.money);
            this.first_deal_time = ObjectUtils.firstNonNull(this.first_deal_time, otherWide.first_deal_time);
            this.lately_deal_time = ObjectUtils.firstNonNull(this.lately_deal_time, otherWide.lately_deal_time);
            this.customer_tag = ObjectUtils.firstNonNull(this.customer_tag, otherWide.customer_tag);
            this.start_status = ObjectUtils.firstNonNull(this.start_status, otherWide.start_status);
//            this.reportAttribute = ObjectUtils.firstNonNull(this.reportAttribute, otherWide.reportAttribute);
            this.rcuservice_versionname = ObjectUtils.firstNonNull(this.rcuservice_versionname, otherWide.rcuservice_versionname);
            this.rcuservice_versioncode = ObjectUtils.firstNonNull(this.rcuservice_versioncode, otherWide.rcuservice_versioncode);
            this.robot_model = ObjectUtils.firstNonNull(this.robot_model, otherWide.robot_model);
            this.robot_manufacturer = ObjectUtils.firstNonNull(this.robot_manufacturer, otherWide.robot_manufacturer);
            this.robot_robotid = ObjectUtils.firstNonNull(this.robot_robotid, otherWide.robot_robotid);
            this.robot_serialnumber = ObjectUtils.firstNonNull(this.robot_serialnumber, otherWide.robot_serialnumber);
            this.robot_softversion = ObjectUtils.firstNonNull(this.robot_softversion, otherWide.robot_softversion);
            this.robot_hardwareversion = ObjectUtils.firstNonNull(this.robot_hardwareversion, otherWide.robot_hardwareversion);
            this.robot_productiondate = ObjectUtils.firstNonNull(this.robot_productiondate, otherWide.robot_productiondate);
            this.robot_version = ObjectUtils.firstNonNull(this.robot_version, otherWide.robot_version);
            this.robot_faceboard = ObjectUtils.firstNonNull(this.robot_faceboard, otherWide.robot_faceboard);
            this.robot_x86 = ObjectUtils.firstNonNull(this.robot_x86, otherWide.robot_x86);
            this.robot_gaussian = ObjectUtils.firstNonNull(this.robot_gaussian, otherWide.robot_gaussian);
            this.robot_mcu = ObjectUtils.firstNonNull(this.robot_mcu, otherWide.robot_mcu);
            this.sca_version = ObjectUtils.firstNonNull(this.sca_version, otherWide.sca_version);
            this.mcsclient_versionname = ObjectUtils.firstNonNull(this.mcsclient_versionname, otherWide.mcsclient_versionname);
            this.mcsclient_versioncode = ObjectUtils.firstNonNull(this.mcsclient_versioncode, otherWide.mcsclient_versioncode);
            this.mcsclient_simiccid = ObjectUtils.firstNonNull(this.mcsclient_simiccid, otherWide.mcsclient_simiccid);
            this.micarray_versionname = ObjectUtils.firstNonNull(this.micarray_versionname, otherWide.micarray_versionname);
            this.rcu_os = ObjectUtils.firstNonNull(this.rcu_os, otherWide.rcu_os);
            this.rcu_model = ObjectUtils.firstNonNull(this.rcu_model, otherWide.rcu_model);
            this.rcu_manufacturer = ObjectUtils.firstNonNull(this.rcu_manufacturer, otherWide.rcu_manufacturer);
            this.rcu_imei = ObjectUtils.firstNonNull(this.rcu_imei, otherWide.rcu_imei);
            this.ue4Client_versionName = ObjectUtils.firstNonNull(this.ue4Client_versionName, otherWide.ue4Client_versionName);
            this.ue4Client_versionCode = ObjectUtils.firstNonNull(this.ue4Client_versionCode, otherWide.ue4Client_versionCode);
            this.ECU_version = ObjectUtils.firstNonNull(this.ECU_version, otherWide.ECU_version);
            this.rcuApp_versionName = ObjectUtils.firstNonNull(this.rcuApp_versionName, otherWide.rcuApp_versionName);
            this.rcuApp_versionCode = ObjectUtils.firstNonNull(this.rcuApp_versionCode, otherWide.rcuApp_versionCode);
            this.robotApp_versionName = ObjectUtils.firstNonNull(this.robotApp_versionName, otherWide.robotApp_versionName);
            this.robotApp_versionCode = ObjectUtils.firstNonNull(this.robotApp_versionCode, otherWide.robotApp_versionCode);
            this.cloudPepperApp_versionName = ObjectUtils.firstNonNull(this.cloudPepperApp_versionName, otherWide.cloudPepperApp_versionName);
            this.cloudPepperApp_versionCode = ObjectUtils.firstNonNull(this.cloudPepperApp_versionCode, otherWide.cloudPepperApp_versionCode);
            this.vendingApp_versionName = ObjectUtils.firstNonNull(this.vendingApp_versionName, otherWide.vendingApp_versionName);
            this.vendingApp_versionCode = ObjectUtils.firstNonNull(this.vendingApp_versionCode, otherWide.vendingApp_versionCode);
            this.digitalBox_versionName = ObjectUtils.firstNonNull(this.digitalBox_versionName, otherWide.digitalBox_versionName);
            this.digitalBox_versionCode = ObjectUtils.firstNonNull(this.digitalBox_versionCode, otherWide.digitalBox_versionCode);
            this.slam_version = ObjectUtils.firstNonNull(this.slam_version, otherWide.slam_version);
            this.pad_version = ObjectUtils.firstNonNull(this.pad_version, otherWide.pad_version);
            this.ikooClient_versionName = ObjectUtils.firstNonNull(this.ikooClient_versionName, otherWide.ikooClient_versionName);
            this.ikooClientversionCode = ObjectUtils.firstNonNull(this.ikooClientversionCode, otherWide.ikooClientversionCode);
            this.library_id = ObjectUtils.firstNonNull(this.library_id, otherWide.library_id);
            this.library_type = ObjectUtils.firstNonNull(this.library_type, otherWide.library_type);
            this.library_from = ObjectUtils.firstNonNull(this.library_from, otherWide.library_from);
            this.library_name = ObjectUtils.firstNonNull(this.library_name, otherWide.library_name);
            this.library_value = ObjectUtils.firstNonNull(this.library_value, otherWide.library_value);
            this.library_status = ObjectUtils.firstNonNull(this.library_status, otherWide.library_status);
        }
    }

    public void mergeReportAttribute(ReportAttribute reportAttribute) {
        if (reportAttribute != null) {
            this.rcuservice_versionname = reportAttribute.rcuservice_versionname;
            this.rcuservice_versioncode = reportAttribute.rcuservice_versioncode;
            this.robot_model = reportAttribute.robot_model;
            this.robot_manufacturer = reportAttribute.robot_manufacturer;
            this.robot_robotid = reportAttribute.robot_robotid;
            this.robot_serialnumber = reportAttribute.robot_serialnumber;
            this.robot_softversion = reportAttribute.robot_softversion;
            this.robot_hardwareversion = reportAttribute.robot_hardwareversion;
            this.robot_productiondate = reportAttribute.robot_productiondate;
            this.robot_version = reportAttribute.robot_version;
            this.robot_faceboard = reportAttribute.robot_faceboard;
            this.robot_x86 = reportAttribute.robot_x86;
            this.robot_gaussian = reportAttribute.robot_gaussian;
            this.robot_mcu = reportAttribute.robot_mcu;
            this.sca_version = reportAttribute.sca_version;
            this.mcsclient_versionname = reportAttribute.mcsclient_versionname;
            this.mcsclient_versioncode = reportAttribute.mcsclient_versioncode;
            this.mcsclient_simiccid = reportAttribute.mcsclient_simiccid;
            this.micarray_versionname = reportAttribute.micarray_versionname;
            this.rcu_os = reportAttribute.rcu_os;
            this.rcu_model = reportAttribute.rcu_model;
            this.rcu_manufacturer = reportAttribute.rcu_manufacturer;
            this.rcu_imei = reportAttribute.rcu_imei;
            this.ue4Client_versionName = reportAttribute.ue4Client_versionName;
            this.ue4Client_versionCode = reportAttribute.ue4Client_versionCode;
            this.ECU_version = reportAttribute.ECU_version;
            this.rcuApp_versionName = reportAttribute.rcuApp_versionName;
            this.rcuApp_versionCode = reportAttribute.rcuApp_versionCode;
            this.robotApp_versionName = reportAttribute.robotApp_versionName;
            this.robotApp_versionCode = reportAttribute.robotApp_versionCode;
            this.cloudPepperApp_versionName = reportAttribute.cloudPepperApp_versionName;
            this.cloudPepperApp_versionCode = reportAttribute.cloudPepperApp_versionCode;
            this.vendingApp_versionName = reportAttribute.vendingApp_versionName;
            this.vendingApp_versionCode = reportAttribute.vendingApp_versionCode;
            this.digitalBox_versionName = reportAttribute.digitalBox_versionName;
            this.digitalBox_versionCode = reportAttribute.digitalBox_versionCode;
            this.slam_version = reportAttribute.slam_version;
            this.pad_version = reportAttribute.pad_version;
            this.ikooClient_versionName = reportAttribute.ikooClient_versionName;
            this.ikooClientversionCode = reportAttribute.ikooClientversionCode;
        }
    }

    public void mergeDevice(Device device) {
        if (device != null) {
            this.asset_code = device.asset_code;
            this.product_type_code = device.product_type_code;
            this.product_type_code_name = device.product_type_code_name;
            this.supplier_code = device.supplier_code;
            this.supplier_code_name = device.supplier_code_name;
            this.product_id = device.product_id;
            this.product_id_name = device.product_id_name;
            this.device_code = device.device_code;
            this.device_name = device.device_name;
            this.device_model = device.device_model;
            this.software_version = device.software_version;
            this.hardware_version = device.hardware_version;
            this.quality_date = device.quality_date;
            this.customer_quality_date = device.customer_quality_date;
            this.device_status = device.status;
            this.product_date = device.product_date;
            this.tenant_code = device.tenant_code;
            this.environment = device.environment;
            this.sku = device.sku;
            this.asset_type = device.asset_type;
            this.roc_delivery_status = device.roc_delivery_status;
            this.is_special = device.is_special;
            this.serial_number = device.serial_number;
            this.operating_status = device.operating_status;
            this.running_status = device.running_status;
            this.asset_status = device.asset_status;
            this.order_overdue = device.order_overdue;
            this.end_time = device.end_time;
            this.update_timestamp = device.update_timestamp;
            this.running_tag = device.running_tag;
            this.project_tag = device.project_tag;
            this.order_type = device.order_type;
            this.country = device.country;
            this.province = device.province;
            this.city = device.city;
            this.district = device.district;
            this.service_address = device.service_address;
            this.contacts = device.contacts;
            this.phone = device.phone;
            this.longitude = device.longitude;
            this.latitude = device.latitude;
            this.if_update_report_location = device.if_update_report_location;
            this.product_category_code = device.product_category_code;
            this.product_category_name = device.product_category_name;
        }
    }

    public void mergeRelationship(Relationship relationship) {
        if (relationship != null) {
            this.id = relationship.id;
            this.tenant_code = relationship.tenant_code;
            this.user_id = relationship.user_id;
            this.rcu_id = relationship.rcu_id;
            this.robot_id = relationship.robot_id;
            this.user_code = relationship.user_code;
            this.rcu_code = relationship.rcu_code;
            this.robot_code = relationship.robot_code;
            this.token = relationship.token;
            this.status = relationship.status;
        }
    }

    public void mergeTenant(Tenant tenant) {
        if (tenant != null) {
            this.vop_id = tenant.vop_id;
            this.name = tenant.name;
            this.code = tenant.code;
            this.phone_area_code = tenant.phone_area_code;
            this.tenant_status = tenant.status;
            this.version = tenant.version;
            this.type = tenant.type;
            this.industry = tenant.industry;
            this.project_name = tenant.project_name;
            this.description = tenant.description;
            this.email = tenant.email;
            this.phone_code = tenant.phone_code;
            this.log = tenant.log;
            this.sub_business = tenant.sub_business;
            this.time_zone = tenant.time_zone;
            this.time_zone_name = tenant.time_zone_name;
            this.address = tenant.address;
            this.customer_id = tenant.customer_id;
            this.salesman = tenant.salesman;
            this.vpn_user_limit = tenant.vpn_user_limit;
            this.vpn_zone_codes = tenant.vpn_zone_codes;
            this.mail = tenant.mail;
        }
    }


    public void mergeConsumer(Consumer consumer) {
        if (consumer != null) {
            this.uuid = consumer.uuid;
            this.father_id = consumer.father_id;
            this.region_code = consumer.region_code;
            this.region_name = consumer.region_name;
            this.nature_code = consumer.nature_code;
            this.nature_name = consumer.nature_name;
            this.source_code = consumer.source_code;
            this.source_name = consumer.source_name;
            this.category_code = consumer.category_code;
            this.category_name = consumer.category_name;
            this.industry_code = consumer.industry_code;
            this.industry_name = consumer.industry_name;
            this.credit_code = consumer.credit_code;
            this.credit_name = consumer.credit_name;
            this.contact_code = consumer.contact_code;
            this.contact_name = consumer.contact_name;
            this.purchase_code = consumer.purchase_code;
            this.purchase_name = consumer.purchase_name;
            this.staff_num = consumer.staff_num;
            this.scale_code = consumer.scale_code;
            this.scale_name = consumer.scale_name;
            this.employment_code = consumer.employment_code;
            this.employment_name = consumer.employment_name;
            this.status_code = consumer.status_code;
            this.status_name = consumer.status_name;
            this.settlement_method_code = consumer.settlement_method_code;
            this.settlement_method_name = consumer.settlement_method_name;
            this.agent = consumer.agent;
            this.fax = consumer.fax;
            this.website = consumer.website;
            this.post_code = consumer.post_code;
            this.consumption_num = consumer.consumption_num;
            this.money = consumer.money;
            this.first_deal_time = consumer.first_deal_time;
            this.lately_deal_time = consumer.lately_deal_time;
            this.customer_tag = consumer.customer_tag;
            this.start_status = consumer.start_status;
        }
    }

    public void mergeUserLibrary(UserLibrary userLibrary) {
        if (userLibrary != null) {
            this.library_id = userLibrary.library_id;
            this.library_type = userLibrary.library_type;
            this.library_from = userLibrary.library_from;
        }
    }


    public void mergeLibrary(Library library) {
        if (library != null) {
            this.library_name = library.library_name;
            this.library_value = library.library_value;
            this.library_status = library.status;
        }
    }


    public String getTenant_code() {
        return tenant_code;
    }

    public String getUser_id() {
        return user_id;
    }

    public String getName() {
        return name;
    }
}
