package com.sentinelpay.ruleengine.domain.model;

public record RuleResult(
        String ruleName,
        boolean triggered,
        String reason,
        int score
) {
    public static RuleResult pass(String ruleName) {
        return new RuleResult(ruleName, false, null, 0);
    }

    public static RuleResult fail(String ruleName, String reason, int score) {
        return new RuleResult(ruleName, true, reason, score);
    }
}