package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.example.constant.PropertiesConstants;
import org.example.model.DataEvent;
import org.example.utils.ExecutionEnvUtil;
import org.example.utils.KafkaConfigUtil;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

@Slf4j
public class FlinkKafkaProducer {

    public static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);
        env.setParallelism(1);

        env.addSource(new SourceFunction<DataEvent>() {
            @Override
            public void run(SourceContext<DataEvent> sourceContext) throws Exception {
                while (true){
                    for (int i=0;i<=100;i++){
                        sourceContext.collect(DataEvent.builder().id1(RandomStringUtils.randomAlphabetic(2)).id2(RandomStringUtils.randomAlphabetic(2))
                                .id3(RandomStringUtils.randomAlphabetic(2)).eventTime(new Date()).value(Integer.parseInt(RandomStringUtils.randomNumeric(3))).build());
                    }
                    Thread.sleep(10000);
                }
            }

            @Override
            public void cancel() {

            }
        }).addSink(new FlinkKafkaProducer011<DataEvent>(
                props.getProperty(PropertiesConstants.KAFKA_BROKERS),
                //"localhost:9092",
                props.getProperty(PropertiesConstants.KAFKA_TOPIC_ID),
                //"event_test",
                new TypeInformationSerializationSchema<DataEvent>(TypeInformation.of(DataEvent.class),env.getConfig())
        )).name("flink-kafka-random-event-Producer");
                //.addSink(new PrintSinkFunction<Event>());
        try {
            env.execute("flink-kafka-random-event-Producer");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
