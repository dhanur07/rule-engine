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
    public StreamsBuilder streamsBuilder(TransactionStreamProcessor processor) {
        StreamsBuilder builder = new StreamsBuilder();
        processor.buildTopology(builder);
        return builder;
    }
}