package cc.yuerblog.canal.demo;

import lombok.Data;

import java.util.Map;

@Data
public class RowEntity {
    private String db;
    private String table;
    private String op;  // SET/DEL
    private String raw_op;  // INSERT/UPDATE/DELETE/...
    private Map<String, String> fields;
}
