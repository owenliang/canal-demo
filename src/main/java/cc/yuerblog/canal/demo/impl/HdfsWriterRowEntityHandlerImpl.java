package cc.yuerblog.canal.demo.impl;

import cc.yuerblog.canal.demo.RowEntity;
import cc.yuerblog.canal.demo.RowEntityHandler;
import com.alibaba.fastjson.JSONObject;

public class HdfsWriterRowEntityHandlerImpl implements RowEntityHandler {
    public void handleRowEntity(RowEntity rowEntity) {
        // TODO: 将rowEntity写入到hdfs，按时间分区目录，按大小滚动文件
        String json = JSONObject.toJSONString(rowEntity);
        System.out.println(json);
    }
}
