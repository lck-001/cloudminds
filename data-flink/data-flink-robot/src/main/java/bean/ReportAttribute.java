package bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;

@Data
public class ReportAttribute {
    public String id;
    //        public String reportAttribute;
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

    public ReportAttribute(String jsonStr) {
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        JSONObject fullDocument = jsonObject.getJSONObject("fullDocument");
        if (fullDocument != null) {
            this.id = fullDocument.getString("_id");
            JSONObject reportAttribute = fullDocument.getJSONObject("reportAttribute");
            if (reportAttribute != null) {
                if (reportAttribute.getJSONObject("rcuService") != null) {
                    this.rcuservice_versionname = reportAttribute.getJSONObject("rcuService").getString("versionName");
                    this.rcuservice_versioncode = reportAttribute.getJSONObject("rcuService").getInteger("versionCode");
                }
                if (reportAttribute.getJSONObject("robot") != null) {
                    this.robot_model = reportAttribute.getJSONObject("robot").getString("model");
                    this.robot_manufacturer = reportAttribute.getJSONObject("robot").getString("manufacturer");
                    this.robot_robotid = reportAttribute.getJSONObject("robot").getString("Robotid");
                    this.robot_serialnumber = reportAttribute.getJSONObject("robot").getString("SerialNumber");
                    this.robot_softversion = reportAttribute.getJSONObject("robot").getString("softversion");
                    this.robot_hardwareversion = reportAttribute.getJSONObject("robot").getString("HardwareVersion");
                    this.robot_productiondate = reportAttribute.getJSONObject("robot").getString("ProductionDate");
                    this.robot_version = reportAttribute.getJSONObject("robot").getString("version");
                    this.robot_faceboard = reportAttribute.getJSONObject("robot").getString("faceBoard");
                    this.robot_x86 = reportAttribute.getJSONObject("robot").getString("x86");
                    this.robot_gaussian = reportAttribute.getJSONObject("robot").getString("gaussian");
                    this.robot_mcu = reportAttribute.getJSONObject("robot").getString("mcu");
                }
                if (reportAttribute.getJSONObject("SCA") != null) {
                    this.sca_version = reportAttribute.getJSONObject("SCA").getString("version");
                }
                if (reportAttribute.getJSONObject("mcsClient") != null) {
                    this.mcsclient_versionname = reportAttribute.getJSONObject("mcsClient").getString("versionName");
                    this.mcsclient_versioncode = reportAttribute.getJSONObject("mcsClient").getInteger("versionCode");
                    this.mcsclient_simiccid = reportAttribute.getJSONObject("mcsClient").getString("simIccid");
                }
                if (reportAttribute.getJSONObject("micArray") != null) {
                    this.micarray_versionname = reportAttribute.getJSONObject("micArray").getString("versionName");
                }
                if (reportAttribute.getJSONObject("rcu") != null) {
                    this.rcu_os = reportAttribute.getJSONObject("rcu").getString("os");
                    this.rcu_model = reportAttribute.getJSONObject("rcu").getString("model");
                    this.rcu_manufacturer = reportAttribute.getJSONObject("rcu").getString("manufacturer");
                    this.rcu_imei = reportAttribute.getJSONObject("rcu").getString("imei");
                }
                if (reportAttribute.getJSONObject("ue4Client") != null) {
                    this.ue4Client_versionName = reportAttribute.getJSONObject("ue4Client").getString("versionName");
                    this.ue4Client_versionCode = reportAttribute.getJSONObject("ue4Client").getInteger("versionCode");
                }
                if (reportAttribute.getJSONObject("ECU") != null) {
                    this.ECU_version = reportAttribute.getJSONObject("ECU").getString("version");
                }
                if (reportAttribute.getJSONObject("rcuApp") != null) {
                    this.rcuApp_versionName = reportAttribute.getJSONObject("rcuApp").getString("versionName");
                    this.rcuApp_versionCode = reportAttribute.getJSONObject("rcuApp").getInteger("versionCode");
                }
                if (reportAttribute.getJSONObject("robotApp") != null) {
                    this.robotApp_versionName = reportAttribute.getJSONObject("robotApp").getString("versionName");
                    this.robotApp_versionCode = reportAttribute.getJSONObject("robotApp").getInteger("versionCode");
                }

                if (reportAttribute.getJSONObject("cloudPepperApp") != null) {
                    this.cloudPepperApp_versionName = reportAttribute.getJSONObject("cloudPepperApp").getString("versionName");
                    this.cloudPepperApp_versionCode = reportAttribute.getJSONObject("cloudPepperApp").getInteger("versionCode");
                }
                if (reportAttribute.getJSONObject("vendingApp") != null) {
                    this.vendingApp_versionName = reportAttribute.getJSONObject("vendingApp").getString("versionName");
                    this.vendingApp_versionCode = reportAttribute.getJSONObject("vendingApp").getInteger("versionCode");
                }

                if (reportAttribute.getJSONObject("digitalBox") != null) {
                    this.digitalBox_versionName = reportAttribute.getJSONObject("digitalBox").getString("versionName");
                    this.digitalBox_versionCode = reportAttribute.getJSONObject("digitalBox").getInteger("versionCode");
                }
                if (reportAttribute.getJSONObject("slam") != null) {
                    this.slam_version = reportAttribute.getJSONObject("slam").getString("version");
                }
                if (reportAttribute.getJSONObject("pad") != null) {
                    this.pad_version = reportAttribute.getJSONObject("pad").getString("version");

                }
                if (reportAttribute.getJSONObject("ikooClient") != null) {
                    this.ikooClient_versionName = reportAttribute.getJSONObject("ikooClient").getString("versionName");
                    this.ikooClientversionCode = reportAttribute.getJSONObject("ikooClient").getInteger("versionCode");
                }
            }
        }
    }

    public String getId() {
        return id;
    }
}








