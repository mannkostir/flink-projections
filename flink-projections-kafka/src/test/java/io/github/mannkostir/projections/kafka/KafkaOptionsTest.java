package io.github.mannkostir.projections.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

import io.github.mannkostir.projections.ProjectionConfigurationException;

class KafkaOptionsTest {
    private static KafkaSourceOptions.Builder source() {
        return KafkaSourceOptions.builder().bootstrapServers("localhost:9092").topic("rows").groupId("test");
    }

    private static KafkaSinkOptions.Builder sink() {
        return KafkaSinkOptions.builder().bootstrapServers("localhost:9092").topic("docs");
    }

    @Test
    void sourceStartsFromCommittedOffsetsFallingBackToEarliest() {
        OffsetsInitializer expected = OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);

        OffsetsInitializer actual = source().build().startingOffsets();

        assertThat(actual.getAutoOffsetResetStrategy()).isEqualTo(expected.getAutoOffsetResetStrategy());
        assertThat(actual).hasSameClassAs(expected);
    }

    @Test
    void sourceHasNoExtraPropertiesByDefault() {
        assertThat(source().build().properties()).isEmpty();
    }

    @Test
    void sourceRequiresBootstrapServers() {
        assertThatThrownBy(() -> source().bootstrapServers(" ").build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("KafkaSourceOptions.bootstrapServers");
    }

    @Test
    void sourceRequiresTopic() {
        assertThatThrownBy(() -> source().topic(null).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("KafkaSourceOptions.topic");
    }

    @Test
    void sourceRequiresGroupId() {
        assertThatThrownBy(() -> source().groupId("").build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("KafkaSourceOptions.groupId");
    }

    @Test
    void sourceRejectsNullStartingOffsets() {
        assertThatThrownBy(() -> source().startingOffsets(null).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("KafkaSourceOptions.startingOffsets");
    }

    @Test
    void sourceRejectsGroupIdProperty() {
        assertThatThrownBy(() -> source().property("group.id", "other").build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("groupId(String)");
    }

    @Test
    void sourceRejectsBootstrapServersProperty() {
        assertThatThrownBy(() -> source().property("bootstrap.servers", "other:9092").build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("bootstrapServers(String)");
    }

    @Test
    void sourcePassesExtraProperties() {
        assertThat(source().property("isolation.level", "read_committed").build().properties())
                .containsEntry("isolation.level", "read_committed");
    }

    @Test
    void sinkIsAtLeastOnceByDefault() {
        assertThat(sink().build().deliveryGuarantee()).isEqualTo(DeliveryGuarantee.AT_LEAST_ONCE);
    }

    @Test
    void sinkHasNoTransactionalIdPrefixByDefault() {
        assertThat(sink().build().transactionalIdPrefix()).isEmpty();
    }

    @Test
    void sinkRequiresBootstrapServers() {
        assertThatThrownBy(() -> sink().bootstrapServers(null).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("KafkaSinkOptions.bootstrapServers");
    }

    @Test
    void sinkRequiresTopic() {
        assertThatThrownBy(() -> sink().topic(" ").build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("KafkaSinkOptions.topic");
    }

    @Test
    void exactlyOnceRequiresTransactionalIdPrefix() {
        assertThatThrownBy(() -> sink().deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE).build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("KafkaSinkOptions.transactionalIdPrefix");
    }

    @Test
    void exactlyOnceKeepsTransactionalIdPrefix() {
        KafkaSinkOptions options = sink().deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE).transactionalIdPrefix("docs").build();

        assertThat(options.transactionalIdPrefix()).contains("docs");
    }

    @Test
    void transactionalIdPrefixWithoutExactlyOnceIsRejected() {
        assertThatThrownBy(() -> sink().transactionalIdPrefix("docs").build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("only used with EXACTLY_ONCE");
    }

    @Test
    void sinkRejectsTransactionalIdProperty() {
        assertThatThrownBy(() -> sink().property("transactional.id", "x").build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("transactionalIdPrefix(String)");
    }

    @Test
    void sinkRejectsBlankPropertyValue() {
        assertThatThrownBy(() -> sink().property("linger.ms", " ").build())
                .isInstanceOf(ProjectionConfigurationException.class)
                .hasMessageContaining("linger.ms");
    }
}
