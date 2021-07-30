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

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Visitor checking whether a filter can be pushed in Lucene.
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
public final class LuceneFilterChecker extends RexVisitorImpl<Boolean> {

  private final Filter filter;

  private LuceneFilterChecker(Filter filter) {
    super(false);
    this.filter = filter;
  }

  @Override public Boolean visitCall(final RexCall call) {
    if (call.getKind().equals(SqlKind.EQUALS)) {
      Boolean isValidInput = call.operands.get(0).accept(new RexVisitorImpl<Boolean>(false) {
        @Override public Boolean visitInputRef(final RexInputRef inputRef) {
          return RelMetadataQuery.instance()
              .getExpressionLineage(filter.getInput(), inputRef) != null;
        }
      });
      Boolean isValidLiteral = call.operands.get(1).accept(new RexVisitorImpl<Boolean>(false) {
        @Override public Boolean visitLiteral(final RexLiteral literal) {
          return SqlTypeName.INTEGER.equals(literal.getType().getSqlTypeName());
        }
      });
      return Boolean.TRUE.equals(isValidInput)
          && Boolean.TRUE.equals(isValidLiteral);
    }
    return Boolean.FALSE;
  }

  /**
   * Returns whether the specified filter can be pushed in Lucene.
   */
  public static boolean isPushable(Filter filter) {
    LuceneFilterChecker checker = new LuceneFilterChecker(filter);
    return Boolean.TRUE.equals(filter.getCondition().accept(checker));
  }
}
