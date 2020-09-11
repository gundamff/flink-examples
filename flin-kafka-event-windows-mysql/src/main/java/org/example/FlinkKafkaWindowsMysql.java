package org.example;

import static org.example.utils.KafkaConfigUtil.buildKafkaProps;

import java.time.ZoneOffset;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.StringUtils;
import org.example.constant.PropertiesConstants;
import org.example.model.DataEvent;
import org.example.model.DataEventHistory;
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
        SingleOutputStreamOperator<DataEventHistory> dataStreamSource = env.addSource(consumer).filter(event -> {
            // 过滤掉错误的
            return event != null && !StringUtils.isNullOrWhitespaceOnly(event.getId());
        }).assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataEvent>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> {
                return event.getEventTime().toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
            })).keyBy("id")
        // @formatter:off
             .timeWindow(Time.seconds(10))
            //.window(TumblingEventTimeWindows.of(Time.seconds(10)))
         // @formatter:on 
            .maxBy("eventTime").map(new MapFunction<DataEvent, DataEventHistory>() {

                private static final long serialVersionUID = -1101914747012198955L;

                @Override
                public DataEventHistory map(DataEvent value) throws Exception {
                    return new DataEventHistory(value);
                }
            });
        dataStreamSource.print();

        dataStreamSource.addSink(JdbcSink.sink("call savedataeventhistory(?,?,?,?)", (ps, t) -> {
            ps.setString(1, t.getId());
            ps.setDate(2, java.sql.Date.valueOf(t.getEventDate()));
            ps.setString(3, t.getHn());
            ps.setInt(4, t.getLastValue());
        }, JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).withMaxRetries(1).build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(
                "jdbc:mysql://localhost:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT")
                .withUsername("root").withPassword("123456").withDriverName("com.mysql.cj.jdbc.Driver").build()))
            .name("flink kafka Windows Mysql");

        env.execute("flink kafka Windows Mysql");
    }
}
