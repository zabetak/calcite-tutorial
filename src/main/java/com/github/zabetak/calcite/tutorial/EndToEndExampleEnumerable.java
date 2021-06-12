package com.github.zabetak.calcite.tutorial;/*
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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

/**
 * An end to end example from an SQL query to a plan in Enumerable convention.
 */
public class EndToEndExampleEnumerable {

  /***/
  public static class Author {
    public final int id;
    public final String fname;
    public final String lname;

    public Author(final int id, final String firstname, final String lastname) {
      this.id = id;
      this.fname = firstname;
      this.lname = lastname;
    }
  }

  /***/
  public static class Book {
    public final int id;
    public final String title;
    public final int year;
    public final Author author;

    public Book(final int id, final String title, final int year, final Author author) {
      this.id = id;
      this.title = title;
      this.year = year;
      this.author = author;
    }
  }

  /***/
  public static class BookStore {
    public static final Author[] AUTHOR = new Author[]{
        new Author(1, "Victor", "Hugo"),
        new Author(2, "Alexandre", "Dumas")
    };
    public static final Book[] BOOK = new Book[]{
        new Book(1, "Les Miserables", 1862, AUTHOR[0]),
        new Book(2, "The Hunchback of Notre-Dame", 1829, AUTHOR[0]),
        new Book(3, "The Last Day of a Condemned Man", 1829, AUTHOR[0]),
        new Book(4, "The three Musketeers", 1844, AUTHOR[1]),
        new Book(5, "The Count of Monte Cristo", 1884, AUTHOR[1])
    };
  }

  public static void main(String[] args) throws Exception {
    CalciteSchema schema = CalciteSchema.createRootSchema(true);
    schema.add("bs", new ReflectiveSchema(new BookStore()));

    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // Create an SQL parser
    SqlParser parser = SqlParser.create(
        "SELECT b.id, b.title, b.\"year\", a.fname || ' ' || a.lname \n"
            + "FROM Book b\n"
            + "LEFT OUTER JOIN Author a ON b.author.id=a.id\n"
            + "ORDER BY b.id\n"
            + "LIMIT 5");
    // Parse the query into an AST
    SqlNode sqlNode = parser.parseQuery();

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList("bs"),
        typeFactory, config);

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT);

    // Validate the initial AST
    SqlNode validNode = validator.validate(sqlNode);

    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
    RelOptCluster cluster = newCluster(typeFactory);
    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());

    // Convert the valid AST into a logical plan
    RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

    // Display the logical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    // Initialize optimizer/planner with the necessary rules
    RelOptPlanner planner = cluster.getPlanner();
    planner.addRule(CoreRules.PROJECT_TO_CALC);
    planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);

    // Define the type of the output plan (in this case we want a physical plan in
    // EnumerableContention)
    logPlan = planner.changeTraits(logPlan,
        cluster.traitSet().replace(EnumerableConvention.INSTANCE));
    planner.setRoot(logPlan);
    // Start the optimization process to obtain the most efficient physical plan based on the
    // provided rule set.
    RelNode phyPlan = planner.findBestExp();

    // Display the physical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    // Obtain the executable plan
    Bindable<Object[]> executablePlan = EnumerableInterpretable.toBindable(
        new HashMap<>(),
        null,
        (EnumerableRel) phyPlan,
        EnumerableRel.Prefer.ARRAY);
    // Run the executable plan using a context simply providing access to the schema
    for (Object[] row : executablePlan.bind(new SchemaOnlyDataContext(schema))) {
      System.out.println(Arrays.toString(row));
    }
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
      return null;
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}
