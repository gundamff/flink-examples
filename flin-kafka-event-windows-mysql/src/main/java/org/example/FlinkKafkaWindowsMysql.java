package org.example;

import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.example.constant.PropertiesConstants;
import org.example.model.DataEvent;
import org.example.utils.ExecutionEnvUtil;

import java.util.Properties;

import static org.example.utils.KafkaConfigUtil.buildKafkaProps;

public class FlinkKafkaWindowsMysql {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);
        Properties props = buildKafkaProps(parameterTool);
        props.put("group.id", "flink kafka Windows Mysql");

        FlinkKafkaConsumer011<DataEvent> consumer = new FlinkKafkaConsumer011<DataEvent>(props.getProperty(PropertiesConstants.KAFKA_TOPIC_ID),
                new TypeInformationSerializationSchema<DataEvent>(TypeInformation.of(DataEvent.class),env.getConfig()), props);

        env.addSource(consumer).keyBy("id1","id2","id3").timeWindow(Time.seconds(60)).max("value").printToErr();

        env.execute("flink kafka To Mysql");
    }
}
