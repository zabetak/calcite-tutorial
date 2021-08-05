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
public final class LuceneFilterChecker {
  // TODO 1. Extend RexVisitorImpl<Boolean>
  // TODO 2. Override and implement visitCall method
  // TODO 3. Ensure call.getKind() corresponds to equals operator
  // TODO 4. Ensure left operand (0) is a valid input ref:
  // a. Create anonymous RexVisitorImpl<Boolean>
  // b. Override visitInputRef method
  // c. Use RelMetadataQuery#getExpressionLineage on filter.getInput() to check it corresponds to
  // a table column
  // TODO 5. Ensure right operand (1) is a valid literal:
  // a. Create anonymous RexVisitorImpl<Boolean>
  // b. Override visitLiteral method
  // c. Use literal.getType().getSqlTypeName to check the type
  // TODO 6. Combine results from visitors in step 4 and 5 and pay attention to potential
  // null values.
  private final Filter filter;

  private LuceneFilterChecker(Filter filter) {
    this.filter = filter;
  }

  /**
   * Returns whether the specified filter can be pushed in Lucene.
   */
  public static boolean isPushable(Filter filter) {
    LuceneFilterChecker checker = new LuceneFilterChecker(filter);
    // TODO 7. Finalize isPushable method by exploiting the visitor
    return Boolean.FALSE;
    //    return Boolean.TRUE.equals(filter.getCondition().accept(checker));
  }
}
