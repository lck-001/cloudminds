package cloudminds.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReportAttribute {
    public Long event_time;
    public String rcu_id;
    public String attribute_name;
    public String attribute_value;
    public String k8s_env_name;
}
