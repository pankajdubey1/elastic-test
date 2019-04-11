package com.elastictest;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;

public class Test {

	public static void main1(String[] args) {
		URL url = Test.class.getClassLoader().getResource("elasticsearch-data");
		System.out.println(url.getFile());
	}

	public static void main(String[] args)
			throws NodeValidationException, IOException, InterruptedException, ExecutionException {
		PutIndexTemplateRequest request = new PutIndexTemplateRequest("test_client");
		request.patterns(Arrays.asList("pattern-1", "log-*"));
		request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 1));

		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		{
			builder.startObject("properties");
			{
				builder.startObject("message");
				{
					builder.field("type", "text");
				}
				builder.endObject();
			}
			builder.endObject();
		}
		builder.endObject();
		request.mapping("abc", builder);
		request.alias(new Alias("test_client"));
		request.order(20);
		request.version(4);
		request.create(true);
		request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
		request.masterNodeTimeout("1m");
		Node node = elasticSearchTestNode();
		node.start();
		Client client = node.client();
		checkAndDeleteTemplate(client, "test_client");
		// ActionFuture<PutIndexTemplateResponse> future =
		// client.admin().indices().putTemplate(request);
		// PutIndexTemplateResponse res = future.get();
		node.close();
	}

	private static void checkAndDeleteTemplate(Client client, String indexName)
			throws InterruptedException, ExecutionException {
		IndicesAdminClient adminClient = client.admin().indices();
		GetIndexTemplatesRequest getRequest = new GetIndexTemplatesRequest(indexName);
		ActionFuture<GetIndexTemplatesResponse> getResponseFuture = adminClient.getTemplates(getRequest);
		GetIndexTemplatesResponse getResponse = getResponseFuture.get();
		if (!CollectionUtils.isEmpty(getResponse.getIndexTemplates())) {
			DeleteIndexTemplateRequest deleteRequest = new DeleteIndexTemplateRequest(indexName);
			ActionFuture<DeleteIndexTemplateResponse> deleteResponseFuture = adminClient.deleteTemplate(deleteRequest);
			System.out.println(deleteResponseFuture.get());
		}
	}

	public static Node elasticSearchTestNode() throws NodeValidationException {
		Settings setting = Settings.builder().put("transport.type", "netty4").put("transport.tcp.port", 9201)
				.put("http.type", "netty4").put("http.port", 9301).put("http.enabled", "true")
				.put("path.home", "test/resources/elasticsearch-data").build();
		return new MyNode(setting, Arrays.asList(Netty4Plugin.class));
	}

	private static class MyNode extends Node {
		public MyNode(Settings preparedSettings, Collection<Class<? extends Plugin>> classpathPlugins) {
			super(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null), classpathPlugins);
		}
	}
}
