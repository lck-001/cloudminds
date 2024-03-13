package bean;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Relationship {
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTenant_code() {
        return tenant_code;
    }

    public void setTenant_code(String tenant_code) {
        this.tenant_code = tenant_code;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getRcu_id() {
        return rcu_id;
    }

    public void setRcu_id(String rcu_id) {
        this.rcu_id = rcu_id;
    }

    public String getRobot_id() {
        return robot_id;
    }

    public void setRobot_id(String robot_id) {
        this.robot_id = robot_id;
    }

    public String getUser_code() {
        return user_code;
    }

    public void setUser_code(String user_code) {
        this.user_code = user_code;
    }

    public String getRcu_code() {
        return rcu_code;
    }

    public void setRcu_code(String rcu_code) {
        this.rcu_code = rcu_code;
    }

    public String getRobot_code() {
        return robot_code;
    }

    public void setRobot_code(String robot_code) {
        this.robot_code = robot_code;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(String update_time) {
        this.update_time = update_time;
    }
}
