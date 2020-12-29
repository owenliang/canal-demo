package cc.yuerblog.canal.demo;

import java.io.IOException;

public interface RowEntityHandler {
    void handleRowEntity(RowEntity rowEntity) throws Exception;
    void handleEmpty() throws Exception;
    void handleExit();
}
