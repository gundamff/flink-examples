package org.example;

import static org.example.utils.KafkaConfigUtil.buildKafkaProps;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.example.constant.PropertiesConstants;
import org.example.model.DataEvent;
import org.example.utils.ExecutionEnvUtil;

public class FlinkKafkaWindowsMysql {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);
        Properties props = buildKafkaProps(parameterTool);
        props.put("group.id", "flink kafka Windows Mysql");

        FlinkKafkaConsumer011<DataEvent> consumer =
            new FlinkKafkaConsumer011<DataEvent>(props.getProperty(PropertiesConstants.KAFKA_TOPIC_ID),
                new TypeInformationSerializationSchema<DataEvent>(TypeInformation.of(DataEvent.class), env.getConfig()),
                props);

        // env.addSource(consumer).keyBy(DataEvent::getId1).window(TumblingEventTimeWindows.of(Time.seconds(5))).max("value").print();

        // https://segmentfault.com/a/1190000023295254
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.addSource(consumer)
            .assignTimestampsAndWatermarks(WatermarkStrategy.<DataEvent>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getEventTime().toInstant(ZoneOffset.ofHours(8)).toEpochMilli()))
            .keyBy("id").timeWindow(Time.seconds(10)).max("eventTime")
            .addSink(JdbcSink.sink(
                "insert into `DataEvent2`(`id`, `eventTime`,`value`) values(?, ?, ?) ON duplicate key update `value`=VALUES(`value`)",
                (ps, t) -> {
                    ps.setString(1, t.getId());
                    ps.setTimestamp(2, new Timestamp(t.getEventTime().toInstant(ZoneOffset.ofHours(8)).toEpochMilli()));
                    ps.setInt(3, t.getValue());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(
                    "jdbc:mysql://localhost:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT")
                    .withUsername("root").withPassword("123456").withDriverName("com.mysql.jdbc.Driver").build()))
            .name("flink kafka Windows Mysql");

        env.execute("flink kafka Windows Mysql");
    }
}
