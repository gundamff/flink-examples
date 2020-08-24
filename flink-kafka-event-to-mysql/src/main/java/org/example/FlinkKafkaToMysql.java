package org.example;

import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.example.constant.PropertiesConstants;
import org.example.model.DataEvent;
import org.example.sinks.SinkToMysql;
import org.example.utils.ExecutionEnvUtil;

import java.util.Properties;

import static org.example.utils.KafkaConfigUtil.buildKafkaProps;

public class FlinkKafkaToMysql {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);
        Properties props = buildKafkaProps(parameterTool);
        props.put("group.id", "flink kafka To Mysql");

        FlinkKafkaConsumer011<DataEvent> consumer = new FlinkKafkaConsumer011<DataEvent>(props.getProperty(PropertiesConstants.KAFKA_TOPIC_ID),
                new TypeInformationSerializationSchema<DataEvent>(TypeInformation.of(DataEvent.class),env.getConfig()), props);

        env.addSource(consumer).addSink(new SinkToMysql()).setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_SINK_PARALLELISM, 1));

        env.execute("flink kafka To Mysql");
    }
}
