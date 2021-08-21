/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.skywalking.library.elasticsearch.requests.search;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.skywalking.library.elasticsearch.requests.search.aggregation.Aggregation;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class SearchBuilder {
    private String[] indices; // TODO: move indices parameter here

    private Integer from;
    private Integer size;
    private Query query;
    private ImmutableList.Builder<Sort> sort;
    private ImmutableList.Builder<Aggregation> aggregations;

    SearchBuilder() {
    }

    public SearchBuilder index(String... indices) {
        requireNonNull(indices, "indices");
        this.indices = indices;
        return this;
    }

    public SearchBuilder from(Integer from) {
        requireNonNull(from, "from");
        checkArgument(from > 0, "from must be positive");
        this.from = from;
        return this;
    }

    public SearchBuilder size(Integer size) {
        requireNonNull(size, "size");
        checkArgument(size > 0, "size must be positive");
        this.size = size;
        return this;
    }

    public SearchBuilder sort(String by, Sort.Order order) {
        checkArgument(!Strings.isNullOrEmpty(by), "by must be positive");
        requireNonNull(order, "order");
        sort().add(new Sort(by, order));
        return this;
    }

    public SearchBuilder query(Query query) {
        requireNonNull(query, "query");
        this.query = query;
        return this;
    }

    public SearchBuilder query(QueryBuilder queryBuilder) {
        return query(queryBuilder.build());
    }

    public SearchBuilder aggregation(Aggregation aggregation) {
        requireNonNull(aggregation, "aggregation");
        aggregations().add(aggregation);
        return this;
    }

    public Search build() {
        return new Search(
            from, size, query, new Sorts(sort.build())
        );
    }

    private ImmutableList.Builder<Sort> sort() {
        if (sort == null) {
            sort = ImmutableList.builder();
        }
        return sort;
    }

    private ImmutableList.Builder<Aggregation> aggregations() {
        if (aggregations == null) {
            aggregations = ImmutableList.builder();
        }
        return aggregations;
    }
}
