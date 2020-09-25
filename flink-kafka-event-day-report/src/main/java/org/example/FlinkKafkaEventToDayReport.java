package org.example;

import static org.example.utils.KafkaConfigUtil.buildKafkaProps;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.extern.slf4j.Slf4j;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import org.example.constant.PropertiesConstants;
import org.example.model.DataEvent;
import org.example.model.DataEventDayReport;
import org.example.utils.ExecutionEnvUtil;

@Slf4j
public class FlinkKafkaEventToDayReport {

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

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        OutputTag<DataEvent> eventTimeTag = new OutputTag("eventTime",TypeInformation.of(DataEvent.class));
        SingleOutputStreamOperator<DataEventDayReport> dataStreamSource = env.addSource(consumer).filter(event -> {
            // 过滤掉错误的
            return event != null && !StringUtils.isNullOrWhitespaceOnly(event.getId());
        }).assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataEvent>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> {
                return event.getEventTime().toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
            })).keyBy(DataEvent::getId)
        // @formatter:off
             .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .allowedLateness(Time.seconds(60)) //允许的误差
                .sideOutputLateData(eventTimeTag)
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10))) //每10秒触发一次
                .apply(new WindowFunction<DataEvent,DataEvent,String, TimeWindow>(){
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DataEvent> iterable, Collector<DataEvent> collector) throws Exception {
                        Optional<DataEvent> data = StreamSupport.stream(
                                iterable.spliterator(), false).collect(Collectors.maxBy(Comparator.comparing(DataEvent::getEventTime)));
                        if(data.isPresent()){
                            collector.collect(data.get());
                        }
                    }
                })

         // @formatter:on
            //.maxBy("eventTime")
                .map(new MapFunction<DataEvent, DataEventDayReport>() {

                private static final long serialVersionUID = -1101914747012198955L;

                @Override
                public DataEventDayReport map(DataEvent value) throws Exception {
                    return new DataEventDayReport(value);
                }
            });
        dataStreamSource.getSideOutput(eventTimeTag).print("sideOut");
        dataStreamSource.print("main");

        dataStreamSource.addSink(JdbcSink.sink("call savedataeventDayReport(?,?,?,?)", (ps, t) -> {
            ps.setString(1, t.getId());
            Timestamp time = Timestamp.valueOf(t.getFiveMinutes());
            System.out.println(time);
            ps.setTimestamp(2, time, Calendar.getInstance());
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
