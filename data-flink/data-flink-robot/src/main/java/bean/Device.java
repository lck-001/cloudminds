package bean;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Device {
    public int id;//  NOTNULLAUTO_INCREMENT
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
    public int status;//  1正常9删除
    public Long product_date;//  生产日期
    public String tenant_code;//  DEFAULTNULL
    public Long create_time;//  创建时间
    public Long update_time;//  修改时间
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


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAsset_code() {
        return asset_code;
    }

    public void setAsset_code(String asset_code) {
        this.asset_code = asset_code;
    }

    public String getProduct_type_code() {
        return product_type_code;
    }

    public void setProduct_type_code(String product_type_code) {
        this.product_type_code = product_type_code;
    }

    public String getProduct_type_code_name() {
        return product_type_code_name;
    }

    public void setProduct_type_code_name(String product_type_code_name) {
        this.product_type_code_name = product_type_code_name;
    }

    public String getSupplier_code() {
        return supplier_code;
    }

    public void setSupplier_code(String supplier_code) {
        this.supplier_code = supplier_code;
    }

    public String getSupplier_code_name() {
        return supplier_code_name;
    }

    public void setSupplier_code_name(String supplier_code_name) {
        this.supplier_code_name = supplier_code_name;
    }

    public int getProduct_id() {
        return product_id;
    }

    public void setProduct_id(int product_id) {
        this.product_id = product_id;
    }

    public String getProduct_id_name() {
        return product_id_name;
    }

    public void setProduct_id_name(String product_id_name) {
        this.product_id_name = product_id_name;
    }

    public String getDevice_code() {
        return device_code;
    }

    public void setDevice_code(String device_code) {
        this.device_code = device_code;
    }

    public String getDevice_name() {
        return device_name;
    }

    public void setDevice_name(String device_name) {
        this.device_name = device_name;
    }

    public String getDevice_model() {
        return device_model;
    }

    public void setDevice_model(String device_model) {
        this.device_model = device_model;
    }

    public String getSoftware_version() {
        return software_version;
    }

    public void setSoftware_version(String software_version) {
        this.software_version = software_version;
    }

    public String getHardware_version() {
        return hardware_version;
    }

    public void setHardware_version(String hardware_version) {
        this.hardware_version = hardware_version;
    }

    public Long getQuality_date() {
        return quality_date;
    }

    public void setQuality_date(Long quality_date) {
        this.quality_date = quality_date;
    }

    public Long getCustomer_quality_date() {
        return customer_quality_date;
    }

    public void setCustomer_quality_date(Long customer_quality_date) {
        this.customer_quality_date = customer_quality_date;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Long getProduct_date() {
        return product_date;
    }

    public void setProduct_date(Long product_date) {
        this.product_date = product_date;
    }

    public String getTenant_code() {
        return tenant_code;
    }

    public void setTenant_code(String tenant_code) {
        this.tenant_code = tenant_code;
    }

    public Long getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Long create_time) {
        this.create_time = create_time;
    }

    public Long getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Long update_time) {
        this.update_time = update_time;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getAsset_type() {
        return asset_type;
    }

    public void setAsset_type(String asset_type) {
        this.asset_type = asset_type;
    }

    public int getRoc_delivery_status() {
        return roc_delivery_status;
    }

    public void setRoc_delivery_status(int roc_delivery_status) {
        this.roc_delivery_status = roc_delivery_status;
    }

    public String getIs_special() {
        return is_special;
    }

    public void setIs_special(String is_special) {
        this.is_special = is_special;
    }

    public String getSerial_number() {
        return serial_number;
    }

    public void setSerial_number(String serial_number) {
        this.serial_number = serial_number;
    }

    public int getOperating_status() {
        return operating_status;
    }

    public void setOperating_status(int operating_status) {
        this.operating_status = operating_status;
    }

    public int getRunning_status() {
        return running_status;
    }

    public void setRunning_status(int running_status) {
        this.running_status = running_status;
    }

    public int getAsset_status() {
        return asset_status;
    }

    public void setAsset_status(int asset_status) {
        this.asset_status = asset_status;
    }

    public int getOrder_overdue() {
        return order_overdue;
    }

    public void setOrder_overdue(int order_overdue) {
        this.order_overdue = order_overdue;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public Long getUpdate_timestamp() {
        return update_timestamp;
    }

    public void setUpdate_timestamp(Long update_timestamp) {
        this.update_timestamp = update_timestamp;
    }

    public String getRunning_tag() {
        return running_tag;
    }

    public void setRunning_tag(String running_tag) {
        this.running_tag = running_tag;
    }

    public String getProject_tag() {
        return project_tag;
    }

    public void setProject_tag(String project_tag) {
        this.project_tag = project_tag;
    }

    public int getOrder_type() {
        return order_type;
    }

    public void setOrder_type(int order_type) {
        this.order_type = order_type;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getService_address() {
        return service_address;
    }

    public void setService_address(String service_address) {
        this.service_address = service_address;
    }

    public String getContacts() {
        return contacts;
    }

    public void setContacts(String contacts) {
        this.contacts = contacts;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public int getIf_update_report_location() {
        return if_update_report_location;
    }

    public void setIf_update_report_location(int if_update_report_location) {
        this.if_update_report_location = if_update_report_location;
    }

    public String getProduct_category_code() {
        return product_category_code;
    }

    public void setProduct_category_code(String product_category_code) {
        this.product_category_code = product_category_code;
    }

    public String getProduct_category_name() {
        return product_category_name;
    }

    public void setProduct_category_name(String product_category_name) {
        this.product_category_name = product_category_name;
    }
}
