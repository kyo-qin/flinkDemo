package org.ota.serv;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

@Slf4j
public class KakfaApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // parse user parameters
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<ObjectNode> myConsumer = new FlinkKafkaConsumer<>("ota"
                , new JSONKeyValueDeserializationSchema(false), properties);
        myConsumer.setStartFromLatest();
        DataStream messageStream = env.addSource(myConsumer);

//        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
//                "ota",                  // target topic
//                new KafkaSerializationSchema<String>() {
//
//                    @Override
//                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
//                        return new ProducerRecord<byte[], byte[]>("ota", element.getBytes());
//                    }
//                }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);   // serialization schem

        messageStream.map(new MapFunction<ObjectNode, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(ObjectNode value) throws Exception {
                return "Kafka and Flink says: " + value;
            }
        }).print();

        env.execute();
    }
}
