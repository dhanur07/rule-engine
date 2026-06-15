package com.sentinelpay.ruleengine.infrastructure.kafka;

import com.sentinelpay.common.events.DecisionEvent;
import com.sentinelpay.common.events.DecisionOutcome;
import com.sentinelpay.common.events.TransactionEvent;
import com.sentinelpay.ruleengine.domain.rules.RuleEngine;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;

@Component
public class TransactionStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransactionStreamProcessor.class);

    private final RuleEngine ruleEngine;
    private final String transactionTopic;
    private final String decisionTopic;
    private final String schemaRegistryUrl;

    public TransactionStreamProcessor(
            RuleEngine ruleEngine,
            @Value("${sentinelpay.kafka.topics.transaction-events}") String transactionTopic,
            @Value("${sentinelpay.kafka.topics.decision-events}") String decisionTopic,
            @Value("${sentinelpay.kafka.schema-registry-url}") String schemaRegistryUrl) {
        this.ruleEngine = ruleEngine;
        this.transactionTopic = transactionTopic;
        this.decisionTopic = decisionTopic;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public void buildTopology(StreamsBuilder builder) {
        // Create Avro serdes
        SpecificAvroSerde<TransactionEvent> transactionSerde = createSerde();
        SpecificAvroSerde<DecisionEvent> decisionSerde = createSerde();

        // Build stream
        KStream<String, TransactionEvent> transactionStream = builder.stream(
                transactionTopic,
                Consumed.with(Serdes.String(), transactionSerde)
        );

        // Process and produce
        transactionStream
                .mapValues(this::evaluateTransaction)
                .to(decisionTopic, Produced.with(Serdes.String(), decisionSerde));

        log.info("Kafka Streams topology built: {} -> {}", transactionTopic, decisionTopic);
    }

    private DecisionEvent evaluateTransaction(TransactionEvent transaction) {
        log.debug("Processing transaction: {}", transaction.getTransactionId());

        RuleEngine.EvaluationResult result = ruleEngine.evaluate(transaction);

        return DecisionEvent.newBuilder()
                .setEventId(com.sentinelpay.common.util.IdGenerator.generate())
                .setTransactionId(transaction.getTransactionId())
                .setTenantId(transaction.getTenantId())
                .setAccountId(transaction.getAccountId())
                .setOutcome(result.outcome())
                .setRiskScore(result.riskScore())
                .setRuleResults(result.ruleResults().stream()
                        .map(r -> com.sentinelpay.common.events.RuleResult.newBuilder()
                                .setRuleId(com.sentinelpay.common.util.IdGenerator.generate())
                                .setRuleName(r.ruleName())
                                .setTriggered(r.triggered())
                                .setReason(r.reason())
                                .setScore(r.score())
                                .build())
                        .toList())
                .setDecidedAt(java.time.Instant.now())
                .setSchemaVersion(1)
                .build();
    }

    private <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> createSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(
                Collections.singletonMap("schema.registry.url", schemaRegistryUrl),
                false
        );
        return serde;
    }
}