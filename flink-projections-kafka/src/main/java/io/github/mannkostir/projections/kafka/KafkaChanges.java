package io.github.mannkostir.projections.kafka;

import java.util.Objects;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.mannkostir.projections.Change;

public final class KafkaChanges {
    private KafkaChanges() {
    }

    public static <T> DataStream<Change<T>> from(
            String name,
            StreamExecutionEnvironment env,
            KafkaSourceOptions options,
            DeserializationSchema<T> format,
            KeySelector<T, String> id,
            DeleteDetection<T> deletes,
            TypeInformation<T> type) {
        KafkaNames.requireValid(name, "Kafka source name");
        Objects.requireNonNull(env, "env");
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(format, "format");
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(deletes, "deletes");
        Objects.requireNonNull(type, "type");
        String uid = KafkaContractNames.sourceUid(name);
        DataStream<T> values = env.fromSource(source(options, format, type), WatermarkStrategy.noWatermarks(), uid, type).uid(uid);
        return deletes.toChanges(name, values, id, type);
    }

    public static <T> DataStreamSink<Change<T>> to(
            String name,
            DataStream<Change<T>> changes,
            KafkaSinkOptions options,
            SerializationSchema<T> format) {
        KafkaNames.requireValid(name, "Kafka sink name");
        Objects.requireNonNull(changes, "changes");
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(format, "format");
        String uid = KafkaContractNames.sinkUid(name);
        return changes.sinkTo(sink(options, format)).uid(uid).name(uid);
    }

    private static <T> KafkaSource<T> source(KafkaSourceOptions options, DeserializationSchema<T> format, TypeInformation<T> type) {
        return KafkaSource.<T>builder()
                .setBootstrapServers(options.bootstrapServers())
                .setTopics(options.topic())
                .setGroupId(options.groupId())
                .setStartingOffsets(options.startingOffsets())
                .setProperties(options.properties())
                .setDeserializer(new ValueRecordDeserializer<>(format, type))
                .build();
    }

    private static <T> KafkaSink<Change<T>> sink(KafkaSinkOptions options, SerializationSchema<T> format) {
        KafkaSinkBuilder<Change<T>> builder = KafkaSink.<Change<T>>builder()
                .setBootstrapServers(options.bootstrapServers())
                .setKafkaProducerConfig(options.properties())
                .setDeliveryGuarantee(options.deliveryGuarantee())
                .setRecordSerializer(new ChangeRecordSerializer<>(options.topic(), format));
        options.transactionalIdPrefix().ifPresent(builder::setTransactionalIdPrefix);
        return builder.build();
    }
}
