package com.sentinelpay.ruleengine.domain.rules;

import com.sentinelpay.common.events.DecisionOutcome;
import com.sentinelpay.common.events.TransactionEvent;
import com.sentinelpay.ruleengine.domain.model.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RuleEngine {

    private static final Logger log = LoggerFactory.getLogger(RuleEngine.class);

    private final List<Rule> rules;
    private final ExecutorService executor;

    public RuleEngine(List<Rule> rules) {
        this.rules = rules;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public EvaluationResult evaluate(TransactionEvent transaction) {
        log.debug("Evaluating transaction: {}", transaction.getTransactionId());

        // Execute all rules in parallel using virtual threads
        List<CompletableFuture<RuleResult>> futures = rules.stream()
                .map(rule -> CompletableFuture.supplyAsync(
                        () -> rule.evaluate(transaction), executor))
                .toList();

        // Wait for all to complete
        List<RuleResult> results = futures.stream()
                .map(CompletableFuture::join)
                .toList();

        // Calculate total risk score
        int totalScore = results.stream()
                .filter(RuleResult::triggered)
                .mapToInt(RuleResult::score)
                .sum();

        // Determine outcome
        DecisionOutcome outcome = determineOutcome(totalScore, results);

        log.info("Transaction {} evaluated: outcome={} score={} triggeredRules={}",
                transaction.getTransactionId(), outcome, totalScore,
                results.stream().filter(RuleResult::triggered).count());

        return new EvaluationResult(outcome, totalScore, results);
    }

    private DecisionOutcome determineOutcome(int totalScore, List<RuleResult> results) {
        // Any critical rule triggered → decline
        boolean hasCriticalFailure = results.stream()
                .anyMatch(r -> r.triggered() && r.score() >= 100);

        if (hasCriticalFailure) {
            return DecisionOutcome.DECLINED;
        }

        // High risk score → manual review
        if (totalScore >= 75) {
            return DecisionOutcome.REVIEW_REQUIRED;
        }

        // Medium risk → manual review
        if (totalScore >= 50) {
            return DecisionOutcome.REVIEW_REQUIRED;
        }

        // Low risk → approve
        return DecisionOutcome.APPROVED;
    }

    public record EvaluationResult(
            DecisionOutcome outcome,
            int riskScore,
            List<RuleResult> ruleResults
    ) {}
}