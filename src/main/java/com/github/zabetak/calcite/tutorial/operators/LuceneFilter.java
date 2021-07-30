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
package com.github.zabetak.calcite.tutorial.operators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/**
 * Implementation of {@link Filter} in {@link LuceneRel#LUCENE} convention.
 *
 * The expression knows how to transform a filter condition in Calcite's {@link RexNode}
 * representation to the respective {@link Query} object in Lucene.
 */
public class LuceneFilter extends Filter implements LuceneRel {
  public LuceneFilter(RelOptCluster cluster, RelNode child, RexNode condition) {
    super(cluster, cluster.traitSetOf(LUCENE), child, condition);
  }

  @Override public Result implement() {
    Result r = ((LuceneRel) getInput()).implement();
    Query q = RexToLuceneTranslator.translate(this);
    return new Result(r.indexPath, new BooleanQuery.Builder()
        .add(q, BooleanClause.Occur.MUST)
        .add(r.query, BooleanClause.Occur.MUST)
        .build());
  }

  @Override public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new LuceneFilter(input.getCluster(), input, condition);
  }
}
