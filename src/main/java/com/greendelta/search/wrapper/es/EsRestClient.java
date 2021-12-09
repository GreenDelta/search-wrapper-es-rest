package com.greendelta.search.wrapper.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import com.greendelta.search.wrapper.SearchClient;
import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.SearchResult;
import com.greendelta.search.wrapper.es.Search.EsRequest;

public class EsRestClient implements SearchClient {

	private final RestHighLevelClient client;
	private final String indexName;

	public EsRestClient(RestHighLevelClient client, String indexName) {
		this.client = client;
		this.indexName = indexName;
	}

	@Override
	public SearchResult<Map<String, Object>> search(SearchQuery searchQuery) {
		try {
			EsRequest request = new RestRequest(client, indexName);
			return Search.run(request, searchQuery);
		} catch (Exception e) {
			e.printStackTrace();
			return new SearchResult<>();
		}
	}

	@Override
	public Set<String> searchIds(SearchQuery searchQuery) {
		EsRequest request = new RestRequest(client, indexName);
		return Search.ids(request, searchQuery);
	}

	@Override
	public void create(Map<String, String> settings) {
		try {
			boolean exists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
			if (exists)
				return;
			String config = settings.get("config");
			String mapping = settings.get("mapping");
			CreateIndexRequest request = new CreateIndexRequest(indexName)
					.settings(Settings.builder().loadFromSource(config, XContentType.JSON))
					.mapping(mapping, XContentType.JSON);
			client.indices().create(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void index(String id, Map<String, Object> content) {
		try {
			client.index(indexRequest(id, content, true), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void index(Map<String, Map<String, Object>> contentsById) {
		bulk(request -> contentsById.keySet()
				.forEach(id -> request.add(indexRequest(id, contentsById.get(id), false))));
	}

	private IndexRequest indexRequest(String id, Map<String, Object> content, boolean refresh) {
		IndexRequest request = new IndexRequest(indexName).id(id).opType(OpType.INDEX).source(content);
		if (refresh) {
			request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return request;
	}

	@Override
	public void update(String id, Map<String, Object> update) {
		try {
			client.update(updateRequest(id, update, true), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void update(String id, String script, Map<String, Object> parameters) {
		try {
			client.update(updateRequest(id, script, parameters, true), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void update(Set<String> ids, Map<String, Object> update) {
		bulk(request -> ids.forEach(id -> request.add(updateRequest(id, update, false))));

	}

	@Override
	public void update(Set<String> ids, String script, Map<String, Object> parameters) {
		bulk(request -> ids.forEach(id -> request.add(updateRequest(id, script, parameters, false))));
	}

	@Override
	public void update(Map<String, Map<String, Object>> updatesById) {
		bulk(request -> updatesById.keySet().forEach(id -> request.add(updateRequest(id, updatesById.get(id), false))));

	}

	private UpdateRequest updateRequest(String id, Map<String, Object> content, boolean refresh) {
		UpdateRequest request = new UpdateRequest(indexName, id).doc(content);
		if (refresh) {
			request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return request;
	}

	private UpdateRequest updateRequest(String id, String script, Map<String, Object> parameters, boolean refresh) {
		UpdateRequest request = new UpdateRequest(indexName, id)
				.script(new Script(ScriptType.INLINE, "painless", script, parameters));
		if (refresh) {
			request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return request;
	}

	@Override
	public void remove(String id) {
		try {
			client.delete(deleteRequest(id, true), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void remove(Set<String> ids) {
		bulk(request -> ids.forEach(id -> request.add(deleteRequest(id, false))));
	}

	private void bulk(Consumer<BulkRequest> createRequests) {
		BulkRequest request = new BulkRequest();
		createRequests.accept(request);
		request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		try {
			client.bulk(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	private DeleteRequest deleteRequest(String id, boolean refresh) {
		DeleteRequest request = new DeleteRequest(indexName, id);
		if (refresh) {
			request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return request;
	}

	@Override
	public boolean has(String id) {
		GetRequest request = new GetRequest(indexName, id);
		try {
			GetResponse response = client.get(request, RequestOptions.DEFAULT);
			if (response == null)
				return false;
			return response.isExists();
		} catch (IOException e) {
			// TODO handle exception
			return false;
		}
	}

	@Override
	public Map<String, Object> get(String id) {
		GetRequest request = new GetRequest(indexName, id);
		try {
			GetResponse response = client.get(request, RequestOptions.DEFAULT);
			if (response == null)
				return null;
			Map<String, Object> source = response.getSource();
			if (source == null || source.isEmpty())
				return null;
			return source;
		} catch (IOException e) {
			// TODO handle exception
			return null;
		}
	}

	@Override
	public List<Map<String, Object>> get(Set<String> ids) {
		MultiGetRequest request = new MultiGetRequest();
		ids.forEach(id -> request.add(indexName, id));
		try {
			MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
			if (response == null)
				return null;
			List<Map<String, Object>> results = new ArrayList<>();
			Iterator<MultiGetItemResponse> it = response.iterator();
			while (it.hasNext()) {
				GetResponse resp = it.next().getResponse();
				if (resp == null)
					continue;
				Map<String, Object> source = resp.getSource();
				if (source == null || source.isEmpty())
					continue;
				results.add(source);
			}
			return results;
		} catch (IOException e) {
			// TODO handle exception
			return null;
		}
	}

	@Override
	public void clear() {
		try {
			boolean exists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
			if (!exists)
				return;
			Settings settings = client.indices()
					.getSettings(new GetSettingsRequest().indices(indexName), RequestOptions.DEFAULT)
					.getIndexToSettings().get(indexName);
			settings = settings.filter(key -> {
				switch (key) {
				case "index.provided_name":
				case "index.creation_date":
				case "index.uuid":
				case "index.version.created":
					return false;
				default:
					return true;
				}
			});
			Map<String, Object> mapping = client.indices()
					.getMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT).mappings()
					.get(indexName).getSourceAsMap();
			client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
			CreateIndexRequest request = new CreateIndexRequest(indexName)
					.settings(settings)
					.mapping(mapping);
			client.indices().create(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void delete() {
		try {
			boolean exists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
			if (!exists)
				return;
			client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

}
