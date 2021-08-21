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

package org.apache.skywalking.library.elasticsearch.requests.factory.v6.codec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.InputStream;
import java.util.Map;
import org.apache.skywalking.library.elasticsearch.requests.IndexRequest;
import org.apache.skywalking.library.elasticsearch.requests.UpdateRequest;
import org.apache.skywalking.library.elasticsearch.requests.factory.Codec;
import org.apache.skywalking.library.elasticsearch.requests.search.BoolQuery;
import org.apache.skywalking.library.elasticsearch.requests.search.RangeQuery;
import org.apache.skywalking.library.elasticsearch.requests.search.TermQuery;

public final class V6Codec implements Codec {
    public static final Codec INSTANCE = new V6Codec();

    public static final ObjectMapper MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .registerModule(
            new SimpleModule()
                .addSerializer(
                    IndexRequest.class,
                    new IndexRequestSerializer()
                )
                .addSerializer(
                    UpdateRequest.class,
                    new UpdateRequestSerializer()
                )
                .addSerializer(
                    RangeQuery.class,
                    new RangeQuerySerializer()
                )
                .addSerializer(
                    TermQuery.class,
                    new TermSerializer()
                )
                .addSerializer(
                    BoolQuery.class,
                    new BoolQuerySerializer()
                )
        )
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    final TypeReference<Map<String, Object>> mapType =
        new TypeReference<Map<String, Object>>() {
        };

    @Override
    public ByteBuf encode(final Object request) throws Exception {
        return Unpooled.wrappedBuffer(MAPPER.writeValueAsBytes(request));
    }

    @Override
    public <T> T decode(final InputStream inputStream,
                        final Class<T> type) throws Exception {
        return MAPPER.readValue(inputStream, type);
    }
}
