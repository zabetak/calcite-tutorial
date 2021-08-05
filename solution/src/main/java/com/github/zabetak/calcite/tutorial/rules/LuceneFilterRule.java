/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.zabetak.calcite.tutorial.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.github.zabetak.calcite.tutorial.operators.LuceneFilter;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

import static com.github.zabetak.calcite.tutorial.operators.LuceneRel.LUCENE;

/**
 * Rule to convert a {@link LogicalFilter} to a {@link LuceneFilter} if possible.
 *
 * The filter can be pushed in Lucene if it is of the following form.
 *
 * <pre>{@code
 * =($0, 154)
 * }</pre>
 *
 * A single equality operator with input reference on the left side and an integer literal on the
 * right side. The input reference should be resolvable to an actual column of the table.
 */
public final class LuceneFilterRule extends ConverterRule {
  LuceneFilterRule(final Config config) {
    super(config);
  }

  @Override public boolean matches(final RelOptRuleCall ruleCall) {
    Filter filter = ruleCall.rel(0);
    RexNode condition = filter.getCondition();
    if (!(condition instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) condition;
    if (call.op.kind.equals(SqlKind.EQUALS) &&
        call.operands.get(0) instanceof RexInputRef &&
        call.operands.get(1) instanceof RexLiteral) {
      RelMetadataQuery mq = ruleCall.getMetadataQuery();
      RexInputRef input = (RexInputRef) call.operands.get(0);
      RexLiteral literal = (RexLiteral) call.operands.get(1);
      Set<RexNode> lineage = mq.getExpressionLineage(filter.getInput(), input);
      return lineage != null && SqlTypeName.INTEGER.equals(literal.getType().getSqlTypeName());
    }
    return false;
  }

  @Override public @Nullable RelNode convert(final RelNode rel) {
    final LogicalFilter filter = (LogicalFilter) rel;
    final RelNode newInput =
        convert(filter.getInput(), filter.getInput().getTraitSet().replace(LUCENE));
    return new LuceneFilter(filter.getCluster(), newInput, filter.getCondition());
  }

  public static final Config DEFAULT = Config.INSTANCE
      .withConversion(LogicalFilter.class, Convention.NONE, LUCENE, "LuceneFilterRule")
      .withRuleFactory(LuceneFilterRule::new);
}
