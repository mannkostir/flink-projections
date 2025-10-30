package org.flink.processing.jobs.base;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flink.io.kafka.KafkaAdmin;
import org.flink.io.kafka.config.KafkaClientConfig;
import org.flink.io.kafka.config.KafkaClientConfig.OffsetStrategy;
import org.flink.io.kafka.topics.KafkaTopic;
import org.flink.processing.jobs.JobSourceInput;
import org.flink.processing.jobs.JobSourceOutput;
import org.flink.processing.records.EnvelopeRecord;
import org.flink.processing.records.SinkRecord;
import org.flink.processing.records.interfaces.IEnvelopeSpecificRecord;
import org.flink.processing.records.serializers.RecordEnvelopeDeserializer;
import org.flink.processing.records.serializers.RecordSerializer;

public abstract class Job { 
    protected final String jobName;
    private final KafkaClientConfig kafkaConfig;
    private final KafkaAdmin kafkaAdmin;
    private final String consumerGroupId;
    private StreamExecutionEnvironment streamEnv;

    protected Job (String jobName, KafkaClientConfig kafkaConfig) {
        this.kafkaAdmin = KafkaAdmin.getInstance(kafkaConfig);
        this.jobName = jobName;
        this.kafkaConfig = kafkaConfig;
        this.consumerGroupId = this.kafkaConfig.namespace() + jobName;
    }

    private <Source> KafkaSourceBuilder<Source> getKafkaSourceBuilder (
            KafkaTopic topic
    ) {
        KafkaSourceBuilder<Source> builder = KafkaSource.<Source>builder()
                                                        .setProperties(this.kafkaConfig.buildConsumerProperties())
                                                        .setBootstrapServers(this.kafkaConfig.bootstrapServers())
                                                        .setTopics(topic.getName())
                                                        .setGroupId(this.consumerGroupId)
                ;

        if (this.kafkaConfig.offsetStrategy() == OffsetStrategy.EARLIEST) {
            builder.setStartingOffsets(OffsetsInitializer.earliest());
        } else if (this.kafkaConfig.offsetStrategy() == OffsetStrategy.LATEST) {
            builder.setStartingOffsets(OffsetsInitializer.latest());
        } else if (this.kafkaConfig.offsetStrategy() == OffsetStrategy.COMMITED) {
            builder.setStartingOffsets(OffsetsInitializer.committedOffsets());
        }

        return builder;
    }

    private <Payload extends SpecificRecordBase> KafkaSinkBuilder<SinkRecord<Payload>> getKafkaSinkBuilder (
    ) {
        KafkaSinkBuilder<SinkRecord<Payload>> builder = KafkaSink.<SinkRecord<Payload>>builder()
                                                  .setBootstrapServers(this.kafkaConfig.bootstrapServers())
                                                  .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                                  .setKafkaProducerConfig(this.kafkaConfig.buildProducerProperties())

                ;

        return builder;
    }

    private <Payload extends SpecificRecordBase> KafkaSource<EnvelopeRecord<Payload>> getKafkaSource (
            KafkaTopic topic, IEnvelopeSpecificRecord<Payload, EnvelopeRecord<Payload>> transformer
    ) {
        this.kafkaAdmin.validateIfTopicExists(topic);

        return this.<EnvelopeRecord<Payload>>getKafkaSourceBuilder(topic)
                   .setDeserializer(new RecordEnvelopeDeserializer<>(transformer, this.kafkaConfig.schemaRegistryUrl()))
                   .build();
    }

    private <Source> KafkaSource<Source> getKafkaSource (
            KafkaTopic topic, KafkaRecordDeserializationSchema<Source> deserializationSchema
    ) {
        this.kafkaAdmin.validateIfTopicExists(topic);

        return this.<Source>getKafkaSourceBuilder(topic)
                   .setDeserializer(deserializationSchema)
                   .build();
    }

    protected final <SinkPayload extends SpecificRecordBase> KafkaSink<SinkRecord<SinkPayload>> getKafkaSink (
            JobSourceOutput<SinkPayload> sourceOutput
    ) {
        this.kafkaAdmin.validateIfTopicExists(sourceOutput.outputTopic);

        return this.<SinkPayload>getKafkaSinkBuilder()
                   .setRecordSerializer(new RecordSerializer<SinkPayload>(sourceOutput.outputTopic.getName(),
                                                               sourceOutput.sourceClass,
                                                               this.kafkaConfig.schemaRegistryUrl()
                   ))
                   .build();
    }

    abstract public void run ()
    throws Exception;

    public void attachStreamEnv (StreamExecutionEnvironment streamEnv) {
        this.streamEnv = streamEnv;
    }

    protected <Source> DataStream<Source> getStreamFromKafkaSource (
            KafkaTopic inputTopic,
            String name,
            KafkaRecordDeserializationSchema<Source> deserializationSchema
    ) {
        KafkaSource<Source> source;

        source = this.getKafkaSource(inputTopic, deserializationSchema);

        return this.streamEnv.fromSource(source, WatermarkStrategy.noWatermarks(), name)
                             .uid("kafka-source_" + this.jobName + "_" + name)
                             .name("kafka-source_" + this.jobName + "_" + name);
    }

    protected <SourcePayload extends SpecificRecordBase> DataStream<EnvelopeRecord<SourcePayload>> getStreamFromKafkaSource (
            JobSourceInput<SourcePayload, EnvelopeRecord<SourcePayload>> sourceInput
    ) {
        KafkaSource<EnvelopeRecord<SourcePayload>> source;

        source = this.getKafkaSource(sourceInput.inputTopic, sourceInput.transformer);

        return this.streamEnv.fromSource(source, WatermarkStrategy.noWatermarks(), sourceInput.name)
                             .uid("kafka-source_" + this.jobName + "_" + sourceInput.name)
                             .name("kafka-source_" + this.jobName + "_" + sourceInput.name);
    }
}
