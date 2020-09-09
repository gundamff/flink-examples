package org.example;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.example.constant.PropertiesConstants;
import org.example.model.DataEvent;
import org.example.utils.ExecutionEnvUtil;
import org.example.utils.KafkaConfigUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkKafkaProducer {

    public static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);
        env.setParallelism(1);

        DataStreamSource<DataEvent> dataStreamSource = env.addSource(new SourceFunction<DataEvent>() {
            @Override
            public void run(SourceContext<DataEvent> sourceContext) throws Exception {
                while (true) {
                    for (int i = 0; i <= 100; i++) {
                        sourceContext.collect(DataEvent.builder()
                            .id(RandomStringUtils.random(5, "ABCDEFGHIJKLMNOPQRST")).eventTime(LocalDateTime.now())
                            .value(Integer.parseInt(RandomStringUtils.randomNumeric(3))).build());
                    }
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        dataStreamSource.addSink(new FlinkKafkaProducer011<DataEvent>(
            props.getProperty(PropertiesConstants.KAFKA_BROKERS),
            // "localhost:9092",
            props.getProperty(PropertiesConstants.KAFKA_TOPIC_ID),
            // "event_test",
            new TypeInformationSerializationSchema<DataEvent>(TypeInformation.of(DataEvent.class), env.getConfig())))
            .name("flink-kafka-random-event-Producer");
        dataStreamSource.print();
        try {
            env.execute("flink-kafka-random-event-Producer");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
