package app.dwm;

import app.function.CustomerDeserialization;
import bean.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.ClickHouseUtil;
import utils.MySQLUtil;

/**
 * TODO　1.同步表
 * flink cdc
 * TODO 2.判断关键字段是否有变化
 * 1.获取主要字段
 * 2.从维度表查询到历史记录（存入redis）
 * 3.获取历史记录相同字段
 *      如果没有： 三元表join
 *      如果有：   对比hash是否相同
 * 4.如果不同   upsert  table() values() where code = ”“
 *
 * 数据 ---> 获取code ---> 从hbase查询历史数据 ---> 判断必要字段是否变更 ---> 变更upsert
 */

public class WideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.获取数据源  并转换为JavaBean对象&提取时间戳生成WaterMark
        //t_user_rcu_robot
        DebeziumSourceFunction<String> relationshipDS = MySQLSource.<String>builder()
                .hostname("172.16.31.1")
                .port(31541)
                .username("bigdata_sync_r")
                .password("bigdata_sync_r")
                .databaseList("roc")
                .tableList("roc.t_user_rcu_robot")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();
//                .hostname("localhost")
//                .port(3306)
//                .username("root")
//                .password("123456")
//                .databaseList("test")
//                .tableList("test.t_user_rcu_robot")
//                .deserializer(new CustomerDeserialization())
//                .serverTimeZone("UTC")
//                .startupOptions(StartupOptions.initial())
//                .build();
        SingleOutputStreamOperator<Relationship> relationshipWithTS = env.addSource(relationshipDS)
                .map(line -> {
                    return JSON.parseObject(line).getJSONObject("after");
                })
                .map(line -> JSON.parseObject(String.valueOf(line), Relationship.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Relationship>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Relationship>() {
                            @Override
                            public long extractTimestamp(Relationship relationship, long recordTimestamp) {
                                return System.currentTimeMillis();
                            }
                        }));
        //t_device
        DebeziumSourceFunction<String> deviceDS = MySQLSource.<String>builder()
                .hostname("172.16.23.4")
                .port(30712)
                .username("root")
                .password("boss123456")
                .databaseList("crss_upms")
                .tableList("crss_upms.t_device")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        SingleOutputStreamOperator<Device> deviceWithTS = env.addSource(deviceDS)
                .map(line -> {
                    return JSON.parseObject(line).getJSONObject("after");
                })
                .map(line -> JSON.parseObject(String.valueOf(line), Device.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Device>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Device>() {
                            @Override
                            public long extractTimestamp(Device deviceWide, long recordTimestamp) {
                                return System.currentTimeMillis();
                            }
                        }));

        //t_tenant
        DebeziumSourceFunction<String> tenantDS = MySQLSource.<String>builder()
                .hostname("172.16.23.4")
                .port(30712)
                .username("root")
                .password("boss123456")
                .databaseList("crss_upms")
                .tableList("crss_upms.tenant")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        SingleOutputStreamOperator<Tenant> tenantWithTS = env.addSource(tenantDS)
                .map(line -> {
                    return JSON.parseObject(line).getJSONObject("after");
                })
                .map(line -> JSON.parseObject(String.valueOf(line), Tenant.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tenant>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tenant>() {
                            @Override
                            public long extractTimestamp(Tenant tenant, long recordTimestamp) {
                                return System.currentTimeMillis();
                            }
                        }));

        //t_customer
        DebeziumSourceFunction<String> customerDS = MySQLSource.<String>builder()
                .hostname("172.16.23.4")
                .port(30712)
                .username("root")
                .password("boss123456")
                .databaseList("crss_upms")
                .tableList("crss_upms.t_customer")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        SingleOutputStreamOperator<Consumer> consumerWithTS = env.addSource(customerDS)
                .map(line -> {
                    return JSON.parseObject(line).getJSONObject("after");
                })
                .map(line -> JSON.parseObject(String.valueOf(line), Consumer.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Consumer>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Consumer>() {
                            @Override
                            public long extractTimestamp(Consumer consumer, long recordTimestamp) {
                                return System.currentTimeMillis();
                            }
                        }));

        //reportAttribute_all
        com.ververica.cdc.debezium.DebeziumSourceFunction<String> reportAttributeDS = MongoDBSource.<String>builder()
                .hosts("172.16.31.12:30310")
                .database("roc")
                .collection("reportAttribute_all")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        SingleOutputStreamOperator<ReportAttribute> reportAttributeWithTS = env.addSource(reportAttributeDS)
//                .map(line ->
//                    //StringEscapeUtils.unescapeJavaScript 去除反斜杠
//                    StringEscapeUtils.unescapeJavaScript(String.valueOf(JSONObject.parseObject(line)))
//                )
                .map(line -> new ReportAttribute(line))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ReportAttribute>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<ReportAttribute>() {
                            @Override
                            public long extractTimestamp(ReportAttribute reportAttribute, long recordTimestamp) {
                                return System.currentTimeMillis();
                            }
                        }));

//        reportAttributeWithTS.print();

        //TODO 3.双流join
        SingleOutputStreamOperator<DeviceWide> deviceWideWithReportAttributeDS = relationshipWithTS.keyBy(Relationship::getRcu_code)
                .intervalJoin(reportAttributeWithTS.keyBy(ReportAttribute::getId))
                .between(Time.seconds(-60), Time.seconds(60)) //生成环境中给的时间给最大延迟时间
                .process(new ProcessJoinFunction<Relationship, ReportAttribute, DeviceWide>() {
                    @Override
                    public void processElement(Relationship relationship, ReportAttribute reportAttribute, Context ctx, Collector<DeviceWide> out) throws Exception {
                        out.collect(new DeviceWide(relationship, reportAttribute));
                    }
                });
        deviceWideWithReportAttributeDS.print("ReportAttribute====");

//        SingleOutputStreamOperator<DeviceWide> deviceWideWithTenantDS = deviceWideWithReportAttributeDS.keyBy(DeviceWide::getTenant_code)
//                .intervalJoin(tenantWithTS.keyBy(Tenant::getCode))
//                .between(Time.seconds(-60), Time.seconds(60)) //生成环境中给的时间给最大延迟时间
//                .process(new ProcessJoinFunction<DeviceWide, Tenant, DeviceWide>() {
//                    @Override
//                    public void processElement(DeviceWide deviceWide, Tenant tenant, Context ctx, Collector<DeviceWide> out) throws Exception {
//                        out.collect(new DeviceWide(deviceWide, tenant));
//                    }
//                });
//        deviceWideWithTenantDS.print("Tenant====");

//        SingleOutputStreamOperator<DeviceWide> deviceWideWithDeviceDS = deviceWideWithTenantDS.keyBy(DeviceWide::getTenant_code)
//                .intervalJoin(deviceWithTS.keyBy(Device::getTenant_code))
//                .between(Time.seconds(-60), Time.seconds(60)) //生成环境中给的时间给最大延迟时间
//                .process(new ProcessJoinFunction<DeviceWide, Device, DeviceWide>() {
//                    @Override
//                    public void processElement(DeviceWide deviceWide,Device device, Context ctx, Collector<DeviceWide> out) throws Exception {
//                        out.collect(new DeviceWide(deviceWide,device));
//                    }
//                });
//        deviceWideWithDeviceDS.print("Device");

        deviceWideWithReportAttributeDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into " + "roc.dwd_robot_info" +
                        "(id,tenant_code,user_id,rcu_id,robot_id,user_code,rcu_code,robot_code,token,status,create_time,update_time,asset_code,product_type_code,product_type_code_name,supplier_code,supplier_code_name,product_id,product_id_name,device_code,device_name,device_model,software_version,hardware_version,quality_date,customer_quality_date,device_status,product_date,environment,sku,asset_type,roc_delivery_status,is_special,serial_number,operating_status,running_status,asset_status,order_overdue,end_time,update_timestamp,running_tag,project_tag,order_type,country,province,city,district,service_address,contacts,phone,longitude,latitude,if_update_report_location,product_category_code,product_category_name,vop_id,name,code,phone_area_code,tenant_status,version,type,industry,project_name,description,email,phone_code,log,sub_business,time_zone,time_zone_name,address,customer_id,salesman,vpn_user_limit,vpn_zone_codes,mail,uuid,father_id,region_code,region_name,nature_code,nature_name,source_code,source_name,category_code,category_name,industry_code,industry_name,credit_code,credit_name,contact_code,contact_name,purchase_code,purchase_name,staff_num,scale_code,scale_name,employment_code,employment_name,status_code,status_name,settlement_method_code,settlement_method_name,agent,fax,website,post_code,consumption_num,money,first_deal_time,lately_deal_time,customer_tag,start_status,rcuservice_versionname,rcuservice_versioncode,robot_model,robot_manufacturer,robot_robotid,robot_serialnumber,robot_softversion,robot_hardwareversion,robot_productiondate,robot_version,robot_faceboard,robot_x86,robot_gaussian,robot_mcu,sca_version,mcsclient_versionname,mcsclient_versioncode,mcsclient_simiccid,micarray_versionname,rcu_os,rcu_model,rcu_manufacturer,rcu_imei,ue4Client_versionName,ue4Client_versionCode,ECU_version,rcuApp_versionName,rcuApp_versionCode,robotApp_versionName,robotApp_versionCode,cloudPepperApp_versionName,cloudPepperApp_versionCode,vendingApp_versionName,vendingApp_versionCode,digitalBox_versionName,digitalBox_versionCode,slam_version,pad_version,ikooClient_versionName,ikooClientversionCode,library_id,library_type,library_from,library_name,library_value,library_status)" +
                        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

//        deviceWideWithReportAttributeDS.addSink(
//                MySQLUtil.getJdbcSink("insert into " + "test02.robot_info" +
//                        "(id,tenant_code,user_id,rcu_id,robot_id,user_code,rcu_code,robot_code,token,status,create_time,update_time,asset_code,product_type_code,product_type_code_name,supplier_code,supplier_code_name,product_id,product_id_name,device_code,device_name,device_model,software_version,hardware_version,quality_date,customer_quality_date,device_status,product_date,environment,sku,asset_type,roc_delivery_status,is_special,serial_number,operating_status,running_status,asset_status,order_overdue,end_time,update_timestamp,running_tag,project_tag,order_type,country,province,city,district,service_address,contacts,phone,longitude,latitude,if_update_report_location,product_category_code,product_category_name,vop_id,name,code,phone_area_code,tenant_status,version,type,industry,project_name,description,email,phone_code,log,sub_business,time_zone,time_zone_name,address,customer_id,salesman,vpn_user_limit,vpn_zone_codes,mail,uuid,father_id,region_code,region_name,nature_code,nature_name,source_code,source_name,category_code,category_name,industry_code,industry_name,credit_code,credit_name,contact_code,contact_name,purchase_code,purchase_name,staff_num,scale_code,scale_name,employment_code,employment_name,status_code,status_name,settlement_method_code,settlement_method_name,agent,fax,website,post_code,consumption_num,money,first_deal_time,lately_deal_time,customer_tag,start_status,rcuservice_versionname,rcuservice_versioncode,robot_model,robot_manufacturer,robot_robotid,robot_serialnumber,robot_softversion,robot_hardwareversion,robot_productiondate,robot_version,robot_faceboard,robot_x86,robot_gaussian,robot_mcu,sca_version,mcsclient_versionname,mcsclient_versioncode,mcsclient_simiccid,micarray_versionname,rcu_os,rcu_model,rcu_manufacturer,rcu_imei,ue4Client_versionName,ue4Client_versionCode,ECU_version,rcuApp_versionName,rcuApp_versionCode,robotApp_versionName,robotApp_versionCode,cloudPepperApp_versionName,cloudPepperApp_versionCode,vendingApp_versionName,vendingApp_versionCode,digitalBox_versionName,digitalBox_versionCode,slam_version,pad_version,ikooClient_versionName,ikooClientversionCode,library_id,library_type,library_from,library_name,library_value,library_status)" +
//                        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
