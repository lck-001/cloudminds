package bean;

import java.io.Serializable;

public class UeLocation implements Serializable{
    public String guid;
    public String robot_id;
    public String tenant_id;
    public String map_id;
    public double mapping3d_x;
    public double mapping3d_y;
    public double mapping3d_z;
    public double mapping3d_angle;
    public double ue_loc_x;
    public double ue_loc_y;
    public double ue_loc_z;
    public double ue_rotate_pitch;
    public double ue_rotate_roll;
    public double ue_rotate_yaw;
    public long event_time;
    public String k8s_env_name;
}
