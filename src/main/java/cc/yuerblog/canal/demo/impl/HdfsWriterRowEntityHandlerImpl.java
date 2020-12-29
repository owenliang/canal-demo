package cc.yuerblog.canal.demo.impl;

import cc.yuerblog.canal.demo.Config;
import cc.yuerblog.canal.demo.RowEntity;
import cc.yuerblog.canal.demo.RowEntityHandler;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsWriterRowEntityHandlerImpl implements RowEntityHandler {
    private FileSystem dfs;
    private String basePath;
    private String logName;
    private Integer sizeLimit;
    private SimpleDateFormat dateFormat;
    private String processName = ManagementFactory.getRuntimeMXBean().getName();

    private Path curPath;
    private String dt;
    private FSDataOutputStream stream;

    public HdfsWriterRowEntityHandlerImpl() throws Exception {
        dfs = FileSystem.get(new Configuration());
        basePath = Config.configuration().getLogBasePath();
        logName = Config.configuration().getLogName();
        dateFormat = new SimpleDateFormat(Config.configuration().getLogDtFormat());
        sizeLimit = Config.configuration().getLogSizeLimit();
    }

    // 关闭当前文件
    private void closeFile() {
        if (stream != null) {
            try {
                System.out.printf("closing file %s\n", curPath.toString());
                stream.close();
            } catch (Exception e) {
            } finally {
                stream = null;
            }
        }
    }

    // 准备写入
    private void prepareForWriting() throws Exception {
        Date d = new Date();
        String curDt = dateFormat.format(d);

        boolean changeFile = false;
        // 时间分区变了
        if (dt == null || !dt.equals(curDt)) {
            changeFile = true;
        }
        // 文件写满
        if (stream != null && stream.getPos() >= sizeLimit) {
            changeFile = true;
        }
        // 文件变更
        if (changeFile) {
            if (stream != null) {
                closeFile();
            }
            Path newPath = new Path(String.format("%s/%s/%s-%s-%d", basePath, curDt, logName, processName, d.getTime()));
            stream = dfs.create(newPath, true);
            curPath = newPath;
            dt = curDt;
            System.out.printf("switching to write %s\n", curPath.toString());
        }
    }

    public void handleRowEntity(RowEntity rowEntity) throws Exception {
        prepareForWriting();

        String db = rowEntity.getDb();
        String table = rowEntity.getTable();
        String op = rowEntity.getOp();
        String rawOp = rowEntity.getRaw_op();
        String fields = JSONObject.toJSONString(rowEntity.getFields());

        String row = String.format("%s\001%s\001%s\001%s\001%s\n", db, table, op, rawOp, fields);
        stream.write(row.getBytes("utf-8"));
    }

    // 时区切换，关闭旧文件
    public void handleEmpty() throws Exception {
        Date d = new Date();
        String curDt = dateFormat.format(d);
        // 时间分区变了
        if (dt != null && !dt.equals(curDt)) {
            closeFile();
        }
    }

    // 退出前关闭文件
    public void handleExit() {
        closeFile();
    }
}
