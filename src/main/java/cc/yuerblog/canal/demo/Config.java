package cc.yuerblog.canal.demo;

import lombok.Data;

import java.io.FileReader;
import java.util.Properties;

@Data
public class Config {
    private String canalHostname;
    private Integer canalPort;
    private String canalDestination;

    private String logBasePath;
    private String logName;
    private String logDtFormat;
    private Integer logSizeLimit;

    private static Config configuration = new Config();

    public static void init(String path) throws Exception{
        Properties props = new Properties();

        FileReader reader = new FileReader(path);
        props.load(reader);

        configuration.setCanalHostname(props.getProperty("canal.hostname"));
        configuration.setCanalPort(Integer.valueOf(props.getProperty("canal.port")));
        configuration.setCanalDestination(props.getProperty("canal.destination"));
        configuration.setLogBasePath(props.getProperty("log.basePath"));
        configuration.setLogName(props.getProperty("log.name"));
        configuration.setLogDtFormat(props.getProperty("log.dtFormat"));
        configuration.setLogSizeLimit(Integer.valueOf(props.getProperty("log.sizeLimit")));
    }

    public static Config configuration() {
        return configuration;
    }
}
