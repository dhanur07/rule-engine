package com.sentinelpay.ruleengine.domain.rules;

import com.sentinelpay.common.events.TransactionEvent;
import com.sentinelpay.ruleengine.domain.model.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class AmountThresholdRule implements Rule {

    private static final Logger log = LoggerFactory.getLogger(AmountThresholdRule.class);

    private final BigDecimal maxAmount;

    public AmountThresholdRule(BigDecimal maxAmount) {
        this.maxAmount = maxAmount;
    }

    @Override
    public RuleResult evaluate(TransactionEvent transaction) {
        BigDecimal amount = transaction.getAmount();

        if (amount.compareTo(maxAmount) > 0) {
            String reason = "Transaction amount %s exceeds threshold %s"
                    .formatted(amount, maxAmount);
            log.warn("Rule triggered: {} for transaction {}", getName(),
                    transaction.getTransactionId());
            return RuleResult.fail(getName(), reason, 50);
        }

        return RuleResult.pass(getName());
    }

    @Override
    public String getName() {
        return "AMOUNT_THRESHOLD";
    }
}