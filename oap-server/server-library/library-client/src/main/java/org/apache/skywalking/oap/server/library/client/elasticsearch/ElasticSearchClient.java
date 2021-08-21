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

package org.apache.skywalking.oap.server.library.client.elasticsearch;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLContext;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.skywalking.apm.util.StringUtil;
import org.apache.skywalking.library.elasticsearch.requests.search.Search;
import org.apache.skywalking.library.elasticsearch.response.Document;
import org.apache.skywalking.library.elasticsearch.response.Documents;
import org.apache.skywalking.oap.server.library.client.Client;
import org.apache.skywalking.oap.server.library.client.healthcheck.DelegatedHealthChecker;
import org.apache.skywalking.oap.server.library.client.healthcheck.HealthCheckable;
import org.apache.skywalking.oap.server.library.util.HealthChecker;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * ElasticSearchClient connects to the ES server by using ES client APIs.
 */
@Slf4j
@RequiredArgsConstructor
public class ElasticSearchClient implements Client, HealthCheckable {
    public static final String TYPE = "type";

    protected final String clusterNodes;

    protected final String protocol;

    private final String trustStorePath;

    @Setter
    private volatile String trustStorePass;

    @Setter
    private volatile String user;

    @Setter
    private volatile String password;

    private final List<IndexNameConverter> indexNameConverters;

    protected volatile RestHighLevelClient client;

    protected DelegatedHealthChecker healthChecker = new DelegatedHealthChecker();

    protected final ReentrantLock connectLock = new ReentrantLock();

    private final int connectTimeout;

    private final int socketTimeout;

    org.apache.skywalking.library.elasticsearch.ElasticSearchClient esClient;

    public ElasticSearchClient(String clusterNodes,
                               String protocol,
                               String trustStorePath,
                               String trustStorePass,
                               String user,
                               String password,
                               List<IndexNameConverter> indexNameConverters,
                               int connectTimeout,
                               int socketTimeout) {
        this.clusterNodes = clusterNodes;
        this.protocol = protocol;
        this.user = user;
        this.password = password;
        this.indexNameConverters = indexNameConverters;
        this.trustStorePath = trustStorePath;
        this.trustStorePass = trustStorePass;
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
        this.esClient = org.apache.skywalking.library.elasticsearch.ElasticSearchClient
            .builder()
            .endpoints(clusterNodes.split(","))
            .protocol(protocol)
            .trustStorePath(trustStorePath)
            .trustStorePass(trustStorePass)
            .username(user)
            .password(password)
            .connectTimeout(connectTimeout)
            .build();
    }

    @Override
    public void connect()
        throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
        CertificateException {
        connectLock.lock();
        try {
            List<HttpHost> hosts = parseClusterNodes(protocol, clusterNodes);
            if (client != null) {
                try {
                    client.close();
                } catch (Throwable t) {
                    log.error("ElasticSearch client reconnection fails based on new config", t);
                }
            }
            client = createClient(hosts);
            client.ping();
            esClient.connect();
        } finally {
            connectLock.unlock();
        }
    }

    protected RestHighLevelClient createClient(
        final List<HttpHost> pairsList)
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
        KeyManagementException {
        RestClientBuilder builder;
        if (StringUtil.isNotEmpty(user) && StringUtil.isNotEmpty(password)) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(user, password));

            if (StringUtil.isEmpty(trustStorePath)) {
                builder = RestClient.builder(pairsList.toArray(new HttpHost[0]))
                                    .setHttpClientConfigCallback(
                                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(
                                            credentialsProvider));
            } else {
                KeyStore truststore = KeyStore.getInstance("jks");
                try (InputStream is = Files.newInputStream(Paths.get(trustStorePath))) {
                    truststore.load(is, trustStorePass.toCharArray());
                }
                SSLContextBuilder sslBuilder =
                    SSLContexts.custom().loadTrustMaterial(truststore, null);
                final SSLContext sslContext = sslBuilder.build();
                builder = RestClient.builder(pairsList.toArray(new HttpHost[0]))
                                    .setHttpClientConfigCallback(
                                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(
                                                                                  credentialsProvider)
                                                                              .setSSLContext(
                                                                                  sslContext));
            }
        } else {
            builder = RestClient.builder(pairsList.toArray(new HttpHost[0]));
        }
        builder.setRequestConfigCallback(
            requestConfigBuilder -> requestConfigBuilder
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
        );

        return new RestHighLevelClient(builder);
    }

    @Override
    public void shutdown() throws IOException {
        client.close();
        esClient.close();
    }

    public static List<HttpHost> parseClusterNodes(String protocol, String nodes) {
        List<HttpHost> httpHosts = new LinkedList<>();
        log.info("elasticsearch cluster nodes: {}", nodes);
        List<String> nodesSplit = Splitter.on(",").omitEmptyStrings().splitToList(nodes);

        for (String node : nodesSplit) {
            String host = node.split(":")[0];
            String port = node.split(":")[1];
            httpHosts.add(new HttpHost(host, Integer.parseInt(port), protocol));
        }

        return httpHosts;
    }

    public boolean createIndex(String indexName) throws IOException {
        indexName = formatIndexName(indexName);

        return esClient.index().create(indexName);
    }

    public boolean updateIndexMapping(String indexName, Map<String, Object> mapping)
        throws IOException {
        indexName = formatIndexName(indexName);

        return esClient.index().putMapping(indexName, TYPE, mapping);
    }

    public Map<String, Object> getIndex(String indexName) throws IOException {
        if (StringUtil.isBlank(indexName)) {
            return new HashMap<>();
        }
        indexName = formatIndexName(indexName);
        try {
            final Map<String, Object> indices = esClient.index().get(indexName);
            if (indices.containsKey(indexName)) {
                // noinspection unchecked
                return (Map<String, Object>) indices.get(indexName);
            }
            return Collections.emptyMap();
        } catch (Exception t) {
            healthChecker.unHealth(t);
            throw t;
        }
    }

    public Collection<String> retrievalIndexByAliases(String aliases) throws IOException {
        aliases = formatIndexName(aliases);

        return esClient.alias().indices(aliases).keySet();
    }

    /**
     * If your indexName is retrieved from elasticsearch through {@link
     * #retrievalIndexByAliases(String)} or some other method and it already contains namespace.
     * Then you should delete the index by this method, this method will no longer concatenate
     * namespace.
     * <p>
     * https://github.com/apache/skywalking/pull/3017
     */
    public boolean deleteByIndexName(String indexName) throws IOException {
        return deleteIndex(indexName, false);
    }

    /**
     * If your indexName is obtained from metadata or configuration and without namespace. Then you
     * should delete the index by this method, this method automatically concatenates namespace.
     * <p>
     * https://github.com/apache/skywalking/pull/3017
     */
    public boolean deleteByModelName(String modelName) throws IOException {
        return deleteIndex(modelName, true);
    }

    protected boolean deleteIndex(String indexName, boolean formatIndexName) throws IOException {
        if (formatIndexName) {
            indexName = formatIndexName(indexName);
        }
        return esClient.index().delete(indexName);
    }

    public boolean isExistsIndex(String indexName) throws IOException {
        indexName = formatIndexName(indexName);

        return esClient.index().exists(indexName);
    }

    public Map<String, Object> getTemplate(String name) throws IOException {
        name = formatIndexName(name);

        try {
            Map<String, Object> templates = esClient.templates().get(name);
            if (templates.containsKey(name)) {
                // noinspection unchecked
                return (Map<String, Object>) templates.get(name);
            }
            return Collections.emptyMap();
        } catch (Exception e) {
            healthChecker.unHealth(e);
            throw e;
        }
    }

    public boolean isExistsTemplate(String indexName) throws IOException {
        indexName = formatIndexName(indexName);

        return esClient.templates().exists(indexName);
    }

    public boolean createOrUpdateTemplate(String indexName, Map<String, Object> settings,
                                          Map<String, Object> mapping, int order)
        throws IOException {
        indexName = formatIndexName(indexName);

        return esClient.templates().createOrUpdate(indexName, settings, mapping, order);
    }

    public boolean deleteTemplate(String indexName) throws IOException {
        indexName = formatIndexName(indexName);

        return esClient.templates().delete(indexName);
    }

    public SearchResponse search(IndexNameMaker indexNameMaker,
                                 SearchSourceBuilder searchSourceBuilder) throws IOException {
        String[] indexNames =
            Arrays.stream(indexNameMaker.make()).map(this::formatIndexName).toArray(String[]::new);
        return doSearch(searchSourceBuilder, indexNames);
    }

    public SearchResponse search(String indexName, SearchSourceBuilder searchSourceBuilder)
        throws IOException {
        indexName = formatIndexName(indexName);
        return doSearch(searchSourceBuilder, indexName);
    }

    public org.apache.skywalking.library.elasticsearch.response.search.SearchResponse search(
        String indexName, Search search)
        throws IOException {
        indexName = formatIndexName(indexName);
        return esClient.search(search, indexName);
    }

    protected SearchResponse doSearch(SearchSourceBuilder searchSourceBuilder,
                                      String... indexNames) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexNames);
        searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
        searchRequest.types(TYPE);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest);
            healthChecker.health();
            return response;
        } catch (Throwable t) {
            healthChecker.unHealth(t);
            handleIOPoolStopped(t);
            throw t;
        }
    }

    protected void handleIOPoolStopped(Throwable t) throws IOException {
        if (!(t instanceof IllegalStateException)) {
            return;
        }
        IllegalStateException ise = (IllegalStateException) t;
        // Fixed the issue described in https://github.com/elastic/elasticsearch/issues/39946
        if (ise.getMessage().contains("I/O reactor status: STOPPED") &&
            connectLock.tryLock()) {
            try {
                connect();
            } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException | CertificateException e) {
                throw new IllegalStateException("Can't reconnect to Elasticsearch", e);
            }
        }
    }

    public Optional<Document> get(String indexName, String id) throws IOException {
        indexName = formatIndexName(indexName);

        return esClient.documents().get(indexName, TYPE, id);
    }

    public boolean existDoc(String indexName, String id) throws IOException {
        indexName = formatIndexName(indexName);

        return esClient.documents().exists(indexName, TYPE, id);
    }

    public Optional<Documents> ids(String indexName, Iterable<String> ids) {
        indexName = formatIndexName(indexName);

        return esClient.documents().mget(indexName, TYPE, ids);
    }

    public Optional<Documents> ids(String indexName, String[] ids) {
        return ids(indexName, Arrays.asList(ids));
    }

    public void forceInsert(String indexName, String id, Map<String, Object> source)
        throws IOException {
        IndexRequestWrapper wrapper = prepareInsert(indexName, id, source);
        Map<String, Object> params = ImmutableMap.of("refresh", "true");
        try {
            esClient.documents().index(wrapper.getRequest(), params);
            healthChecker.health();
        } catch (Throwable t) {
            healthChecker.unHealth(t);
            throw t;
        }
    }

    public void forceUpdate(String indexName, String id, Map<String, Object> source)
        throws IOException {
        UpdateRequestWrapper wrapper = prepareUpdate(indexName, id, source);
        Map<String, Object> params = ImmutableMap.of("refresh", "true");
        try {
            esClient.documents().update(wrapper.getRequest(), params);
            healthChecker.health();
        } catch (Throwable t) {
            healthChecker.unHealth(t);
            throw t;
        }
    }

    public IndexRequestWrapper prepareInsert(String indexName, String id,
                                             Map<String, Object> source) {
        indexName = formatIndexName(indexName);
        return new IndexRequestWrapper(indexName, TYPE, id, source);
    }

    public UpdateRequestWrapper prepareUpdate(String indexName, String id,
                                              Map<String, Object> source) {
        indexName = formatIndexName(indexName);
        return new UpdateRequestWrapper(indexName, TYPE, id, source);
    }

    public int delete(String indexName, String timeBucketColumnName, long endTimeBucket)
        throws IOException {
        indexName = formatIndexName(indexName);
        Map<String, Object> params = Collections.singletonMap("conflicts", "proceed");
        String query = "" +
            "{" +
            "  \"query\": {" +
            "    \"range\": {" +
            "      \"" + timeBucketColumnName + "\": {" +
            "        \"lte\": " + endTimeBucket +
            "      }" +
            "    }" +
            "  }" +
            "}";

        esClient.documents().delete(indexName, TYPE, query, params);

        return 0; // TODO
    }

    public org.apache.skywalking.library.elasticsearch.bulk.BulkProcessor createBulkProcessor(
        int bulkActions, int flushInterval,
        int concurrentRequests) {
        return esClient.bulkProcessor()
                       .bulkActions(bulkActions)
                       .flushInterval(Duration.ofSeconds(flushInterval))
                       .concurrentRequests(concurrentRequests)
                       .build();
    }

    protected BulkProcessor.Listener createBulkListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                log.debug("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    log.warn(
                        "Bulk [{}] executed with failures:[{}]", executionId,
                        response.buildFailureMessage()
                    );
                } else {
                    log.info(
                        "Bulk execution id [{}] completed in {} milliseconds, size: {}",
                        executionId, response.getTook()
                                             .getMillis(),
                        request
                            .requests()
                            .size()
                    );
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.error("Failed to execute bulk", failure);
            }
        };
    }

    public String formatIndexName(String indexName) {
        for (final IndexNameConverter indexNameConverter : indexNameConverters) {
            indexName = indexNameConverter.convert(indexName);
        }
        return indexName;
    }

    @Override
    public void registerChecker(HealthChecker healthChecker) {
        this.healthChecker.register(healthChecker);
    }
}
