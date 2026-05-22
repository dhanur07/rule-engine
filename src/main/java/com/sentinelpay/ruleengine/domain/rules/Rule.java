package com.sentinelpay.ruleengine.domain.rules;

import com.sentinelpay.common.events.TransactionEvent;
import com.sentinelpay.ruleengine.domain.model.RuleResult;

public interface Rule {
    RuleResult evaluate(TransactionEvent transaction);
    String getName();
}