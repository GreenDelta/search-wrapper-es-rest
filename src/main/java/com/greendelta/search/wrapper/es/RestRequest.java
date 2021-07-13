package com.greendelta.search.wrapper.es;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.greendelta.search.wrapper.es.Search.EsRequest;

class RestRequest implements EsRequest {

	private final RestHighLevelClient client;
	private final SearchRequest request;

	RestRequest(RestHighLevelClient client, String indexName) {
		this.client = client;
		this.request = new SearchRequest(indexName);
	}

	@Override
	public void setFrom(int from) {
		request.source().from(from);
	}

	@Override
	public void setSize(int size) {
		request.source().size(size);
	}

	@Override
	public void addSort(String field, SortOrder order) {
		request.source().sort(field, order);
	}

	@Override
	public void addAggregation(AggregationBuilder aggregation) {
		request.source().aggregation(aggregation);
	}

	@Override
	public void setQuery(QueryBuilder query) {
		request.source().query(query);
		request.source().trackTotalHits(true);
	}

	@Override
	public RestResponse execute() throws IOException {
		return new RestResponse(client.search(request, RequestOptions.DEFAULT));
	}

}
