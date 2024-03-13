package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Library {
        public String id;
        public String tenant_code;
        public String library_name;
        public String library_type;
        public String library_value;
        public int status;

        public String getTenant_code() {
                return tenant_code;
        }
}
