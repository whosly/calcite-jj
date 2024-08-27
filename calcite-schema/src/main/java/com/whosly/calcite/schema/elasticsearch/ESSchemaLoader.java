package com.whosly.calcite.schema.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whosly.calcite.schema.ISchemaLoader;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.schema.Schema;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

public class ESSchemaLoader implements ISchemaLoader {

    public static Schema loadSchema(String hostname, int port) {
        // 1.构建ElasticsearchSchema对象，在Calcite中，不同数据源对应不同Schema
        RestClient restClient = RestClient.builder(new HttpHost(hostname, port)).build();

        return new ElasticsearchSchema(
                restClient, new ObjectMapper(), "teachers");
    }
}
