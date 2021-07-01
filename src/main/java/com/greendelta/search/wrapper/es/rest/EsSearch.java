package com.greendelta.search.wrapper.es.rest;

import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.linearDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.sort.SortOrder;

import com.greendelta.search.wrapper.Conjunction;
import com.greendelta.search.wrapper.LinearDecayFunction;
import com.greendelta.search.wrapper.MultiSearchFilter;
import com.greendelta.search.wrapper.SearchFilter;
import com.greendelta.search.wrapper.SearchFilterValue;
import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.SearchResult;
import com.greendelta.search.wrapper.SearchSorting;
import com.greendelta.search.wrapper.aggregations.RangeAggregation;
import com.greendelta.search.wrapper.aggregations.SearchAggregation;
import com.greendelta.search.wrapper.aggregations.TermsAggregation;
import com.greendelta.search.wrapper.aggregations.results.AggregationResult;
import com.greendelta.search.wrapper.aggregations.results.AggregationResultBuilder;
import com.greendelta.search.wrapper.aggregations.results.AggregationResultEntry;
import com.greendelta.search.wrapper.score.Score;

class EsSearch {

	private static final String RANGE_TYPE = "range";
	private static final String NESTED_TYPE = "nested";

	static SearchResult<Map<String, Object>> search(SearchQuery searchQuery, RestHighLevelClient client,
			String indexName) {
		try {
			SearchRequest request = request(searchQuery, client, indexName);
			return search(client, request, searchQuery);
		} catch (Exception e) {
			e.printStackTrace();
			return new SearchResult<>();
		}
	}

	static Set<String> searchIds(SearchQuery searchQuery, RestHighLevelClient client, String indexName) {
		SearchRequest request = request(searchQuery, client, indexName);
		return searchIds(client, request, searchQuery);
	}

	private static SearchRequest request(SearchQuery searchQuery, RestHighLevelClient client, String indexName) {
		SearchRequest request = new SearchRequest(indexName);
		setupPaging(request, searchQuery);
		setupSorting(request, searchQuery);
		setupQuery(request, searchQuery);
		request.source().trackTotalHits(true);
		return request;
	}

	private static void setupPaging(SearchRequest request, SearchQuery searchQuery) {
		int start = (searchQuery.getPage() - 1) * searchQuery.getPageSize();
		if (start > 0) {
			request.source().from(start);
		}
		if (!searchQuery.isPaged()) {
			request.source().size(10000);
		} else {
			if (searchQuery.getPageSize() > 0) {
				request.source().size(searchQuery.getPageSize());
			} else {
				request.source().size(SearchQuery.DEFAULT_PAGE_SIZE);
			}
		}
	}

	private static void setupSorting(SearchRequest request, SearchQuery searchQuery) {
		for (Entry<String, SearchSorting> entry : searchQuery.getSortBy().entrySet()) {
			SortOrder value = entry.getValue() == SearchSorting.ASC ? SortOrder.ASC : SortOrder.DESC;
			request.source().sort(entry.getKey(), value);
		}
	}

	private static void setupQuery(SearchRequest request, SearchQuery searchQuery) {
		for (SearchAggregation aggregation : searchQuery.getAggregations()) {
			request.source().aggregation(EsAggregations.getBuilder(aggregation));
		}
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		QueryBuilder current = null;
		int count = 0;
		for (SearchFilter filter : searchQuery.getFilters()) {
			List<SearchAggregation> aggregations = searchQuery.getAggregationsByField(filter.field);
			for (SearchAggregation aggregation : aggregations) {
				QueryBuilder q = toQuery(filter, aggregation);
				if (q == null)
					continue;
				count++;
				current = q;
				query.must(q);
			}
		}
		for (MultiSearchFilter filter : searchQuery.getMultiFilters()) {
			QueryBuilder q = toQuery(filter);
			if (q == null)
				continue;
			count++;
			current = q;
			query.must(q);
		}
		QueryBuilder finalQuery = null;
		if (count == 0) {
			finalQuery = QueryBuilders.matchAllQuery();
		} else if (count == 1) {
			finalQuery = current;
		} else {
			finalQuery = query;
		}
		finalQuery = addScores(finalQuery, searchQuery);
		request.source().query(finalQuery);
	}

	private static QueryBuilder addScores(QueryBuilder query, SearchQuery searchQuery) {
		if (searchQuery.getScores().isEmpty())
			return query;
		List<FilterFunctionBuilder> functions = new ArrayList<>();
		for (int i = 0; i < searchQuery.getScores().size(); i++) {
			Score score = searchQuery.getScores().get(i);
			functions.add(new FilterFunctionBuilder(scriptFunction(EsScript.from(score))));
		}
		for (int i = 0; i < searchQuery.getFunctions().size(); i++) {
			LinearDecayFunction function = searchQuery.getFunctions().get(i);
			functions.add(new FilterFunctionBuilder(linearDecayFunction(function.fieldName, function.origin,
					function.scale, function.offset, function.decay)));
		}
		return functionScoreQuery(query, functions.toArray(new FilterFunctionBuilder[functions.size()]));
	}

	private static QueryBuilder toQuery(SearchFilter filter, SearchAggregation aggregation) {
		if (filter.values.isEmpty())
			return null;
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		QueryBuilder last = null;
		int count = 0;
		for (SearchFilterValue value : filter.values) {
			if (value.value instanceof String && value.value.toString().isEmpty())
				continue;
			if (aggregation == null) {
				last = getQuery(filter.field, value);
			} else {
				last = EsAggregations.getQuery(aggregation, value);
			}
			if (filter.conjunction == Conjunction.AND) {
				query.must(last);
			} else if (filter.conjunction == Conjunction.OR) {
				query.should(last);
			}
			count++;
		}
		if (count == 0)
			return null;
		if (count == 1)
			return last;
		return query;
	}

	private static QueryBuilder toQuery(MultiSearchFilter filter) {
		if (filter.values.isEmpty())
			return null;
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		QueryBuilder last = null;
		int count = 0;
		for (String field : filter.fields) {
			int innerCount = 0;
			BoolQueryBuilder inner = QueryBuilders.boolQuery();
			for (SearchFilterValue value : filter.values) {
				if (value.value instanceof String && value.value.toString().isEmpty())
					continue;
				last = getQuery(field, value);
				if (filter.conjunction == Conjunction.AND) {
					inner.must(last);
				} else if (filter.conjunction == Conjunction.OR) {
					inner.should(last);
				}
				innerCount++;
			}
			if (innerCount == 1) {
				query.should(last);
			} else {
				query.should(inner);
				last = inner;
			}
			count++;
		}
		if (count == 0)
			return null;
		if (count == 1)
			return last;
		return query;
	}

	private static QueryBuilder getQuery(String field, SearchFilterValue value) {
		switch (value.type) {
		case RANGE:
			Object[] range = (Object[]) value.value;
			RangeQueryBuilder rangeQuery = rangeQuery(field);
			if (range[0] != null) {
				rangeQuery.from(range[0]);
			}
			if (range[1] != null) {
				rangeQuery.to(range[1]);
			}
			return rangeQuery;
		case WILDCART:
			return wildcardQuery(field, value.value.toString());
		case PHRASE:
			if (!(value.value instanceof Collection))
				return matchPhraseQuery(field, value.value);
			Collection<?> phrases = (Collection<?>) value.value;
			BoolQueryBuilder query = QueryBuilders.boolQuery();
			for (Object p : phrases) {
				query.should(matchPhraseQuery(field, p));
			}
			return query;
		case TERM:
			if (!(value.value instanceof Collection))
				return termQuery(field, value.value);
			Collection<?> terms = (Collection<?>) value.value;
			return termsQuery(field, terms);
		default:
			return matchAllQuery();
		}
	}

	private static SearchResult<Map<String, Object>> search(RestHighLevelClient client, SearchRequest request,
			SearchQuery searchQuery) {
		try {
			SearchResult<Map<String, Object>> result = new SearchResult<>();
			SearchResponse response = null;
			boolean doContinue = true;
			long totalHits = 0;
			while (doContinue) {
				if (!searchQuery.isPaged()) {
					request.source().from(result.data.size());
				}
				response = client.search(request, RequestOptions.DEFAULT);
				SearchHit[] hits = response.getHits().getHits();
				for (SearchHit hit : hits) {
					if (searchQuery.getFullResult()) {
						result.data.add(hit.getSourceAsMap());
					} else {
						result.data.add(Collections.singletonMap("documentId", hit.getId()));
					}
				}
				totalHits = response.getHits().getTotalHits().value;
				doContinue = !searchQuery.isPaged() && result.data.size() != totalHits;
			}
			if (response.getAggregations() != null) {
				result.aggregations.addAll(toResults(response.getAggregations().asList()));
			}
			result.resultInfo.count = result.data.size();
			extendResultInfo(result, totalHits, searchQuery);
			return result;
		} catch (Exception e) {
			// TODO handle exception
			SearchResult<Map<String, Object>> result = new SearchResult<>();
			extendResultInfo(result, 0, searchQuery);
			return result;
		}
	}

	private static Set<String> searchIds(RestHighLevelClient client, SearchRequest request, SearchQuery searchQuery) {
		try {
			Set<String> ids = new HashSet<>();
			SearchResponse response = null;
			boolean doContinue = true;
			long totalHits = 0;
			while (doContinue) {
				if (!searchQuery.isPaged()) {
					request.source().from(ids.size());
				}
				response = client.search(request, RequestOptions.DEFAULT);
				SearchHit[] hits = response.getHits().getHits();
				for (SearchHit hit : hits) {
					ids.add(hit.getId());
				}
				totalHits = response.getHits().getTotalHits().value;
				doContinue = !searchQuery.isPaged() && ids.size() != totalHits;
			}
			return ids;
		} catch (Exception e) {
			// TODO handle exception
			return new HashSet<>();
		}
	}

	private static List<AggregationResult> toResults(List<Aggregation> aggregations) {
		List<AggregationResult> results = new ArrayList<>();
		for (Aggregation aggregation : aggregations) {
			if (!aggregation.getType().equals(NESTED_TYPE)) {
				results.add(toResult(aggregation));
			} else {
				results.addAll(toResults(((ParsedNested) aggregation).getAggregations().asList()));
			}
		}
		return results;
	}

	private static AggregationResult toResult(Aggregation aggregation) {
		AggregationResultBuilder builder = new AggregationResultBuilder();
		builder.name(aggregation.getName()).type(mapType(aggregation.getType()));
		putSpecificData(aggregation, builder);
		return builder.build();
	}

	private static String mapType(String type) {
		if (type == null)
			return null;
		switch (type) {
		case StringTerms.NAME:
		case LongTerms.NAME:
		case DoubleTerms.NAME:
			return TermsAggregation.TYPE;
		case RANGE_TYPE:
			return RangeAggregation.TYPE;
		default:
			return "UNKNOWN";
		}
	}

	private static void putSpecificData(Aggregation aggregation, AggregationResultBuilder builder) {
		long totalCount = 0;
		switch (aggregation.getType()) {
		case StringTerms.NAME:
		case LongTerms.NAME:
		case DoubleTerms.NAME:
			for (Bucket bucket : getTermBuckets(aggregation)) {
				builder.addEntry(new AggregationResultEntry(bucket.getKeyAsString(), bucket.getDocCount()));
				totalCount += bucket.getDocCount();
			}
			builder.totalCount(totalCount);
			break;
		case RANGE_TYPE:
			for (org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket : ((ParsedRange) aggregation)
					.getBuckets()) {
				Object[] data = new Object[] { bucket.getFrom(), bucket.getTo() };
				builder.addEntry(new AggregationResultEntry(bucket.getKeyAsString(), bucket.getDocCount(), data));
				totalCount += bucket.getDocCount();
			}
			builder.totalCount(totalCount);
			break;
		}
	}

	private static List<? extends Bucket> getTermBuckets(Aggregation aggregation) {
		switch (aggregation.getType()) {
		case StringTerms.NAME:
			return ((ParsedStringTerms) aggregation).getBuckets();
		case LongTerms.NAME:
			return ((LongTerms) aggregation).getBuckets();
		case DoubleTerms.NAME:
			return ((DoubleTerms) aggregation).getBuckets();
		default:
			return new ArrayList<>();
		}
	}

	private static void extendResultInfo(SearchResult<Map<String, Object>> result, long totalHits,
			SearchQuery searchQuery) {
		result.resultInfo.totalCount = totalHits;
		result.resultInfo.currentPage = searchQuery.getPage();
		result.resultInfo.pageSize = searchQuery.getPageSize();
		long totalCount = result.resultInfo.totalCount;
		if (searchQuery.getPageSize() != 0) {
			int pageCount = (int) totalCount / searchQuery.getPageSize();
			if ((totalCount % searchQuery.getPageSize()) != 0) {
				pageCount = 1 + pageCount;
			}
			result.resultInfo.pageCount = pageCount;
		}
	}

}
