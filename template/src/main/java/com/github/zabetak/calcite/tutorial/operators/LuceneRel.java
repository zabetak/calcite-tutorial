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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.lucene.search.Query;

/**
 * Relational expression representing Apache Lucene specific operations.
 */
public interface LuceneRel extends RelNode {
  Convention LUCENE = new Convention.Impl("LUCENE", LuceneRel.class);

  /**
   * Implements the current expression and returns the result.
   *
   * The result can be combined with other relational expressions of the same interface and contains
   * all the necessary information to extract data from a Lucene index.
   */
  Result implement();

  /**
   * Result of implementing a Lucene relational expression.
   */
  class Result {
    /**
     * Path to the index location in the filesystem.
     */
    public final String indexPath;
    /**
     * Query for extracting the data from the index.
     */
    public final Query query;

    public Result(String indexPath, Query query) {
      this.indexPath = indexPath;
      this.query = query;
    }
  }
}
