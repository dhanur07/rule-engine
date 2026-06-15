package com.sentinelpay.ruleengine.config;

import com.sentinelpay.ruleengine.domain.rules.AmountThresholdRule;
import com.sentinelpay.ruleengine.domain.rules.Rule;
import com.sentinelpay.ruleengine.domain.rules.RuleEngine;
import com.sentinelpay.ruleengine.infrastructure.kafka.TransactionStreamProcessor;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.math.BigDecimal;
import java.util.List;

@Configuration
@EnableKafkaStreams
public class StreamsConfiguration {

    @Bean
    public RuleEngine ruleEngine(
            @Value("${sentinelpay.rules.max-transaction-amount}") BigDecimal maxAmount) {
        List<Rule> rules = List.of(
                new AmountThresholdRule(maxAmount)
        );
        return new RuleEngine(rules);
    }

    @Bean
    public Object buildTopology(StreamsBuilder streamsBuilder,
                                TransactionStreamProcessor processor) {
        processor.buildTopology(streamsBuilder);
        return new Object();
    }
}