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
package com.github.zabetak.calcite.tutorial;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Query processor for running TPC-H queries over Apache Lucene.
 */
public class LuceneQueryProcessor {

  /**
   * Plans and executes an SQL query in the file specified
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: processor SQL_FILE");
      System.exit(-1);
    }
    String sqlQuery = new String(Files.readAllBytes(Paths.get(args[0])), StandardCharsets.UTF_8);

    // TODO 1. Create the root schema and type factory
    // TODO 2. Create the data type for each TPC-H table
    // TODO 3. Add the TPC-H table to the schema

    // TODO 4. Create an SQL parser
    // TODO 5. Parse the query into an AST
    // TODO 6. Print and check the AST

    // TODO 7. Configure and instantiate the catalog reader
    // TODO 8. Create the SQL validator using the standard operator table and default configuration

    // TODO 9. Validate the initial AST

    // TODO 10. Create the optimization cluster to maintain planning information
    // TODO 11. Configure and instantiate the converter of the AST to Logical plan
    // - No view expansion (use NOOP_EXPANDER)
    // - Standard expression normalization (use StandardConvertletTable.INSTANCE)
    // - Default configuration (SqlToRelConverter.config())

    // TODO 12. Convert the valid AST into a logical plan
    // TODO 13. Display the logical plan with explain attributes

    // TODO 14. Initialize optimizer/planner with the necessary rules

    // TODO 15. Define the type of the output plan (in this case we want a physical plan in
    // EnumerableContention)

    // TODO 16. Start the optimization process to obtain the most efficient physical plan based on
    // the provided rule set.

    // TODO 17. Display the physical plan

    // TODO 18. Compile generated code and obtain the executable program

    // TODO 19. Run the program using a context simply providing access to the schema and print
    // results
    long start = System.currentTimeMillis();
    long finish = System.currentTimeMillis();
    System.out.println("Elapsed time " + (finish - start) + "ms");
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

  /**
   * A simple data context only with schema information.
   */
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return new JavaTypeFactoryImpl();
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}
