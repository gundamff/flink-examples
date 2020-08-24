package org.example;

import lombok.extern.slf4j.Slf4j;
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

    private static final String[] ID1S = new String[]{
            "A","B","C","d","E","F","G","H"
    };

    private static final String[] ID2S = new String[]{
            "甲","乙","丙","丁","戊","己","庚","辛"
    };

    private static final String[] ID3S = new String[]{
            "1","2","3","4","5","6","7","8"
    };

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
                        sourceContext.collect(DataEvent.builder().id1(ID1S[random.nextInt(ID1S.length)]).id2(ID2S[random.nextInt(ID2S.length)])
                                .id3(ID3S[random.nextInt(ID3S.length)]).eventTime(new Date()).value(random.nextInt()).build());
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
