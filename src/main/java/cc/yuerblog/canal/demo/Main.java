package cc.yuerblog.canal.demo;

import cc.yuerblog.canal.demo.impl.HdfsWriterRowEntityHandlerImpl;

public class Main {
    public static void main(String[] args) {
        CanalConsumer consumer = new CanalConsumer(new HdfsWriterRowEntityHandlerImpl());
        consumer.run();
    }
}
