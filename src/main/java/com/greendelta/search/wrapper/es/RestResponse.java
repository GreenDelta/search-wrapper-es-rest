package com.greendelta.search.wrapper.es;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

import com.greendelta.search.wrapper.es.Search.EsResponse;

class RestResponse implements EsResponse {

	private final SearchResponse response;

	RestResponse(SearchResponse response) {
		this.response = response;
	}

	@Override
	public SearchHit[] getHits() {
		return response.getHits().getHits();
	}

	@Override
	public long getTotalHits() {
		return response.getHits().getTotalHits().value;
	}

	@Override
	public List<Aggregation> getAggregations() {
		if (response.getAggregations() == null)
			return new ArrayList<>();
		return response.getAggregations().asList();
	}

	@Override
	public List<? extends Bucket> getTermBuckets(Aggregation aggregation) {
		switch (aggregation.getType()) {
		case StringTerms.NAME:
			return ((ParsedStringTerms) aggregation).getBuckets();
		case LongTerms.NAME:
			return ((ParsedLongTerms) aggregation).getBuckets();
		case DoubleTerms.NAME:
			return ((ParsedDoubleTerms) aggregation).getBuckets();
		default:
			return new ArrayList<>();
		}
	}

	public List<? extends org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> getRangeBuckets(
			Aggregation aggregation) {
		return ((ParsedRange) aggregation).getBuckets();
	}

}