package cloudminds.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReportMetrics {
    public String event_time;
    public String tenant_id;
    public String robot_id;
    public String rcu_id;
    public String user_id;
    public String robot_type;
    public String rod_type;
    public String service_code;
    public String attribute_name;
    public String attribute_value;
    public String k8s_env_name;
}
