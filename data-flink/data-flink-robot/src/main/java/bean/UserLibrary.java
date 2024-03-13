package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLibrary {
    public String id;
    public String user_id;
    public String library_type;
    public String library_id;
    public int library_from;

    public String getUser_id() {
        return user_id;
    }
}
