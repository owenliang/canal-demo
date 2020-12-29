package cc.yuerblog.canal.demo;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanalConsumer {
    private RowEntityHandler handler;

    CanalConsumer(RowEntityHandler handler) {
        this.handler = handler;
    }

    public void run() {
        while (true) {
            int batchSize = 1000;
            CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(Config.configuration().getCanalHostname(),
                    Config.configuration().getCanalPort()), Config.configuration().getCanalDestination(), "", "");
            try {
                connector.connect();
                while (true) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        try {
                            handler.handleEmpty();
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        procEntries(message.getEntries());
                    }
                    connector.ack(batchId); // 提交确认
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                connector.disconnect();
            }
        }
    }

    private void procEntries(List<CanalEntry.Entry> entrys) throws Exception {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            String binlog = entry.getHeader().getLogfileName();
            String offset = String.valueOf(entry.getHeader().getLogfileOffset());
            String db = entry.getHeader().getSchemaName();
            String table = entry.getHeader().getTableName();

            System.out.println(String.format("binlog[%s:%s] , name[%s,%s] , eventType : %s", binlog, offset, db, table, eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                userCallback(eventType, db, table, rowData);
            }
        }
    }

    private void userCallback(CanalEntry.EventType eventType, String db, String table, CanalEntry.RowData row) throws Exception {
        RowEntity rowEntity = new RowEntity();
        rowEntity.setDb(db);
        rowEntity.setTable(table);
        rowEntity.setRaw_op(eventType.toString());

        List<CanalEntry.Column> columns = null;

        if (eventType == CanalEntry.EventType.DELETE) {
            rowEntity.setOp("DEL");
            columns = row.getBeforeColumnsList();
        } else {
            rowEntity.setOp("SET");
            columns = row.getAfterColumnsList();
        }

        Map<String, String> fields = new HashMap<String, String>();
        for (CanalEntry.Column column : columns) {
            fields.put(column.getName(), column.getValue());
        }
        rowEntity.setFields(fields);

        this.handler.handleRowEntity(rowEntity);
    }
}
