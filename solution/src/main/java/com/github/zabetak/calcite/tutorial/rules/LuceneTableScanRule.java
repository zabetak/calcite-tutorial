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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

import com.github.zabetak.calcite.tutorial.LuceneTable;
import com.github.zabetak.calcite.tutorial.operators.LuceneTableScan;

import java.util.Collections;

import static com.github.zabetak.calcite.tutorial.operators.LuceneRel.LUCENE;

/**
 * Rule to convert a {@link LogicalTableScan} to a {@link LuceneTableScan} if possible.

 * The expression can be converted to a {@link LuceneTableScan} if the table corresponds to a Lucene
 * index.
 */
public final class LuceneTableScanRule extends ConverterRule {
  @Override public RelNode convert(final RelNode rel) {
    final LogicalTableScan scan = (LogicalTableScan) rel;
    final LuceneTable table = scan.getTable().unwrap(LuceneTable.class);
    if (table != null) {
      return
          new LuceneTableScan(
              scan.getCluster(),
              scan.getCluster().traitSetOf(LUCENE),
              Collections.emptyList(),
              scan.getTable());
    }
    return null;
  }

  public LuceneTableScanRule(final Config config) {
    super(config);
  }

  public static final Config DEFAULT = Config.INSTANCE
      .withConversion(LogicalTableScan.class, Convention.NONE,
          LUCENE, "LuceneTableScanRule")
      .withRuleFactory(LuceneTableScanRule::new);
}
