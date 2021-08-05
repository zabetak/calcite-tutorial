<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# Apache Calcite

A tutorial of [Apache Calcite]((http://calcite.apache.org))
for the [BOSS'21 VLDB workshop](https://boss-workshop.github.io/boss-2021/).

In this tutorial, we demonstrate the main components of Calcite and how they interact with each
other. To do this we build, step-by-step, a fully fledged query processor for data residing
in Lucene indexes, and gradually introduce various extensions covering some common use-cases
appearing in practice.

The project has three modules:
* `indexer`, containing the necessary code to populate some sample dataset(s) into Lucene to
demonstrate the capabilities of the query processor; 
* `solution`, containing the material of the tutorial fully implemented along with a few unit tests
ensuring the correctness of the code;   
* `template`, containing only the skeleton and documentation of selected classes, which the
attendees can use to follow the real-time implementation of the Lucene query processor.

## Requirements

* JDK version >= 8

## Quickstart

To compile the project, run:

    ./mvnw package -DskipTests 

To load/index the TPC-H dataset in Lucene, run:

    java -jar indexer/target/indexer-1.0-SNAPSHOT-jar-with-dependencies.jar
    
The indexer creates the data under `target/tpch` directory. The TPC-H dataset was generated using
the dbgen command line utility (`dbgen -s 0.001`) provided in the original
[TPC-H tools](http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) bundle.

To execute SQL queries over the data in Lucene, and get a feeling of how the finished query
processor looks like, run: 

    java -jar solution/target/solution-1.0-SNAPSHOT-jar-with-dependencies.jar SIMPLE queries/tpch/Q0.sql
    java -jar solution/target/solution-1.0-SNAPSHOT-jar-with-dependencies.jar ADVANCED queries/tpch/Q0.sql
    java -jar solution/target/solution-1.0-SNAPSHOT-jar-with-dependencies.jar PUSHDOWN queries/tpch/Q0.sql

The finished query processor provides three execution modes, representing the three main sections
which are covered in this tutorial.

You can use one of the predefined queries under `queries/tpch` directory or create a new file
and write your own. 

In `SIMPLE` mode, the query processor does not do any advanced optimization and shows how easy it
is to build an adapter from scratch with very few lines of customized code by relying on the
built-in operators of the `EnumerableConvention` and the `ScannableTable` interface.

In `ADVANCED` mode, the query processor is able to combine operators with different characteristics
demonstrating the most common implementation pattern of an adapter and sets the bases for building
federation query engines using Calcite. In this mode, we combine two kinds of operators using the
built-in `EnumerableConvention` and the custom `LuceneRel#LUCENE` convention along with some basic
optimization rules.  

In `PUSHDOWN` mode, the query processor combines operators with different characteristics and is
also capable of pushing simple filtering conditions to the underlying engine by introducing
custom rules, expression transformations, and additional operators.