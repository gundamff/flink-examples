package org.example;

import static org.example.utils.KafkaConfigUtil.buildKafkaProps;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.Properties;

import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.StringUtils;
import org.example.constant.PropertiesConstants;
import org.example.model.DataEvent;
import org.example.utils.ExecutionEnvUtil;

public class FlinkKafkaToMysql {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        // env.setParallelism(1);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(5000);
        Properties props = buildKafkaProps(parameterTool);
        props.put("group.id", "flink kafka To Mysql");

        FlinkKafkaConsumer011<DataEvent> consumer =
            new FlinkKafkaConsumer011<DataEvent>(props.getProperty(PropertiesConstants.KAFKA_TOPIC_ID),
                new TypeInformationSerializationSchema<DataEvent>(TypeInformation.of(DataEvent.class), env.getConfig()),
                props);

        SingleOutputStreamOperator<DataEvent> dataStreamSource = env.addSource(consumer).filter(event -> {
            // 过滤掉错误的
            return event != null && !StringUtils.isNullOrWhitespaceOnly(event.getId());
        });
        // 控制台输出
        dataStreamSource.print();
        // 使用jdbcSink
        dataStreamSource.addSink(getJdbcSink()).name("kafka to mysql");

        env.execute("flink kafka To Mysql");
    }

    private static SinkFunction<DataEvent> getJdbcSink() {
        return JdbcSink.sink(
            "insert into `DataEvent`(`id`, `eventTime`,`value`) values(?, ?, ?) ON duplicate key update `value`=VALUES(`value`)",
            (ps, t) -> {
                ps.setString(1, t.getId());
                ps.setTimestamp(2, new Timestamp(t.getEventTime().toInstant(ZoneOffset.ofHours(8)).toEpochMilli()));
                ps.setInt(3, t.getValue());
            }, JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).withMaxRetries(1).build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(
                "jdbc:mysql://localhost:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT")
                .withUsername("root").withPassword("123456").withDriverName("com.mysql.cj.jdbc.Driver").build());
    }
}
