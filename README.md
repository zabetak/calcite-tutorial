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
other. To do this we build, step-by-step, a fully fledged query processor for in-memory data.
The `EndToEndExampleEnumerable` class contains the minimum pieces required to build a simple query
processor from scratch. We explain the responsibilities of each component and then implement various
extensions around these components covering some common use-cases appearing in practice.

## Requirements

* JDK version >= 8

## Quickstart

To compile the project, run:

    ./mvnw package

To run the end-to-end example:

    java -cp target/calcite-tutorial-1.0-SNAPSHOT-jar-with-dependencies.jar EndToEndExampleEnumerable
