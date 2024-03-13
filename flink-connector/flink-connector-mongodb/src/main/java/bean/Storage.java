package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Storage implements Serializable {
    public String _id;
    public String tenantId;
    public String robotId;
    public String robotType;
    public String userId;
    public String storage;
    public String business;
    public String fileId;
    public String fileDot;
    public String fileExt;
    public Long filePos;
    public Long fileSize;
    public String createTime;
    public String updateTime;
    public Integer status;
    public String _class;
    public String bucket;
    public String endpoint;
    public String fileKey;
}
