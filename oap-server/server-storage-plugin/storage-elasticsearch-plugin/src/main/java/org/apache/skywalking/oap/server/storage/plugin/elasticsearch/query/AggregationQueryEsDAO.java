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
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.skywalking.library.elasticsearch.requests.search.BoolQueryBuilder;
import org.apache.skywalking.library.elasticsearch.requests.search.Query;
import org.apache.skywalking.library.elasticsearch.requests.search.RangeQueryBuilder;
import org.apache.skywalking.library.elasticsearch.requests.search.Search;
import org.apache.skywalking.library.elasticsearch.requests.search.SearchBuilder;
import org.apache.skywalking.library.elasticsearch.requests.search.aggregation.Aggregation;
import org.apache.skywalking.library.elasticsearch.response.search.SearchResponse;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.query.enumeration.Order;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.input.TopNCondition;
import org.apache.skywalking.oap.server.core.query.type.KeyValue;
import org.apache.skywalking.oap.server.core.query.type.SelectedRecord;
import org.apache.skywalking.oap.server.core.storage.query.IAggregationQueryDAO;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.EsDAO;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.IndexController;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class AggregationQueryEsDAO extends EsDAO implements IAggregationQueryDAO {

    public AggregationQueryEsDAO(ElasticSearchClient client) {
        super(client);
    }

    @Override
    public List<SelectedRecord> sortMetrics(final TopNCondition condition,
                                            final String valueColumnName,
                                            final Duration duration,
                                            final List<KeyValue> additionalConditions)
        throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        final RangeQueryBuilder basicQuery = Query.range(Metrics.TIME_BUCKET)
                                                  .lte(duration.getEndTimeBucket())
                                                  .gte(duration.getStartTimeBucket());
        final SearchBuilder search = Search.builder();

        final boolean asc = condition.getOrder().equals(Order.ASC);
        final String tableName =
            IndexController.LogicIndicesRegister.getPhysicalTableName(condition.getName());

        if (CollectionUtils.isEmpty(additionalConditions)
            && IndexController.LogicIndicesRegister.isMetricTable(condition.getName())) {
            final org.apache.skywalking.library.elasticsearch.requests.search.BoolQueryBuilder
                boolQuery = Query.bool();
            boolQuery.must(basicQuery)
                     .must(Query.term(
                         IndexController.LogicIndicesRegister.METRIC_TABLE_NAME,
                         condition.getName()
                     ));
            search.query(boolQuery);
        } else if (CollectionUtils.isEmpty(additionalConditions)) {
            search.query(basicQuery);
        } else if (CollectionUtils.isNotEmpty(additionalConditions)
            && IndexController.LogicIndicesRegister.isMetricTable(condition.getName())) {
            final org.apache.skywalking.library.elasticsearch.requests.search.BoolQueryBuilder
                boolQuery = Query.bool();
            boolQuery.must(Query.term(
                IndexController.LogicIndicesRegister.METRIC_TABLE_NAME,
                condition.getName()
            ));
            additionalConditions.forEach(additionalCondition -> boolQuery
                .must(Query.term(
                    additionalCondition.getKey(),
                    additionalCondition.getValue()
                )));
            boolQuery.must(basicQuery);
            search.query(boolQuery);
        } else {
            final BoolQueryBuilder boolQuery = Query.bool();
            additionalConditions.forEach(additionalCondition -> boolQuery
                .must(Query.term(
                    additionalCondition.getKey(),
                    additionalCondition.getValue()
                )));
            boolQuery.must(basicQuery);
            search.query(boolQuery);
        }

        search.aggregation(
            Aggregation.terms(Metrics.ENTITY_ID)
                       .field(Metrics.ENTITY_ID)
                       .order(
                           org.apache.skywalking.library.elasticsearch.requests.search.aggregation.BucketOrder.aggregation(
                               valueColumnName, asc))
                       .size(condition.getTopN())
                       .subAggregation(Aggregation.avg(valueColumnName).field(valueColumnName))
                       .build());

        SearchResponse response = getClient().search(tableName, search.build());

        List<SelectedRecord> topNList = new ArrayList<>();
        // TODO
        // Terms idTerms = response.getAggregations().get(Metrics.ENTITY_ID);
        // for (Terms.Bucket termsBucket : idTerms.getBuckets()) {
        //     SelectedRecord record = new SelectedRecord();
        //     record.setId(termsBucket.getKeyAsString());
        //     Avg value = termsBucket.getAggregations().get(valueColumnName);
        //     record.setValue(String.valueOf((long) value.getValue()));
        //     topNList.add(record);
        // }

        return topNList;
    }

}
