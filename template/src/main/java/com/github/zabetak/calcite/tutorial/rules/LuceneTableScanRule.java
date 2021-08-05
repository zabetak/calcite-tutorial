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

import org.apache.calcite.rel.logical.LogicalTableScan;

import com.github.zabetak.calcite.tutorial.operators.LuceneTableScan;

/**
 * Rule to convert a {@link LogicalTableScan} to a {@link LuceneTableScan} if possible.
 * The expression can be converted to a {@link LuceneTableScan} if the table corresponds to a Lucene
 * index.
 */
public final class LuceneTableScanRule {
  // TODO 1. Extend Converter rule
  // TODO 2. Implement convert method
  // TODO 2a. Check for table class
  // TODO 2b. Change operator convention
  // TODO 3. Create default rule config

}
