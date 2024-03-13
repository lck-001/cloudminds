package bean;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class VidioInfo implements Serializable {
    public String id;
    public String tenant_id;
    public String robot_id;
    public String robot_type;
    public String user_id;
    public String storage;
    public String business;
    public String file_id;
    public String file_dot;
    public String file_ext;
    public String file_pos;
    public String file_size;
    public String create_time;
    public String update_time;
    public Integer status;
    public String k8s_env_name = "bj-prod-232";
}
