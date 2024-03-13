package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NatsData implements Serializable {
    public String guid;
    public String robotid;
    public String tenantid;
    public String mapid;
    public int mapping3d_x;
    public int mapping3d_y;
    public int mapping3d_z;
    public int mapping3d_angle;
    public int ue_loc_x;
    public int ue_loc_y;
    public int ue_loc_z;
    public int ue_rotate_pitch;
    public int ue_rotate_roll;
    public int ue_rotate_yaw;
    public long timestamp;
}
