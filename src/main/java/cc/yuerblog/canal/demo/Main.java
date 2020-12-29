package cc.yuerblog.canal.demo;

import cc.yuerblog.canal.demo.impl.HdfsWriterRowEntityHandlerImpl;

/**
 * create table if not exists ods_binlog_userlog(
 *     db_name string,
 *     table_name string,
 *     op string,
 *     raw_op string,
 *     fields string
 * )
 * partitioned by(dt string)
 * stored as textfile
 * location '/warehouse/safe/ods/ods_binlog_userlog';
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Config.init(args[0]);

        CanalConsumer consumer = new CanalConsumer(new HdfsWriterRowEntityHandlerImpl());
        consumer.run();
    }
}