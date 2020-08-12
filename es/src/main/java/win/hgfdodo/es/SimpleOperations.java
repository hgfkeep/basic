package win.hgfdodo.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SimpleOperations {
    private final RestHighLevelClient client;
    public final static String ID_FIELD = "_id";
    private final static String CLASSIFY_FIELD = "classify";

    public SimpleOperations(RestHighLevelClient client) {
        this.client = client;
    }

    public IndexResponse index(String index, Map<String, Object> origin) throws IOException {
        IndexRequest request = new IndexRequest(index).id((String) origin.get(ID_FIELD));
        origin.remove(ID_FIELD);
        request.source(origin);
        return this.client.index(request, RequestOptions.DEFAULT);
    }

    public Cancellable indexAsync(final String index, Map<String, Object> origin) throws IOException {
        IndexRequest request = new IndexRequest(index).id((String) origin.get(ID_FIELD));
        origin.remove(ID_FIELD);
        request.source(origin);
        return this.client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            public void onResponse(IndexResponse indexResponse) {
                System.out.println("Index Response:" + indexResponse);
            }

            public void onFailure(Exception e) {
                System.out.println("Index Error:" + e);
            }
        });
    }

    public GetRequest get(final String index, final String id) {
        return new GetRequest(index, id);
    }

    /**
     * 执行 部分更新 请求
     *
     * @param index  索引
     * @param origin 原始数据，带es id 字段
     * @return
     * @throws IOException
     */
    public UpdateResponse update(String index, Map<String, Object> origin) throws IOException {
        UpdateRequest request = request(index, origin);
        return this.client.update(request, RequestOptions.DEFAULT);
    }

    /**
     * 生成  部分更新 请求
     *
     * @param index  索引
     * @param origin 带 id的部分更新请求数据
     * @return
     */
    private UpdateRequest request(String index, Map<String, Object> origin) {

        UpdateRequest request = new UpdateRequest(index, (String) origin.get(ID_FIELD));
        Map<String, Object> toUpdate = new HashMap<String, Object>(1);
        toUpdate.put(CLASSIFY_FIELD, origin.get(CLASSIFY_FIELD));
        request.doc(toUpdate).retryOnConflict(3);
        return request;
    }

    public void bulkRequest(String index) throws IOException {
        BulkRequest bulkRequest = new BulkRequest(index);
        String id = UUID.randomUUID().toString();
        System.out.println("id=" + id);
        Map<String, Object> indexMap = new HashMap<String, Object>();
        indexMap.put("age", 18);
        indexMap.put("say", "hello world");
        indexMap.put("name", "xxxx");
        System.out.println("1 "+client.index(new IndexRequest(index).id(id).source(indexMap), RequestOptions.DEFAULT));
        System.out.println("2 "+client.update(new UpdateRequest(index, id).upsert("additional", "hello additional"), RequestOptions.DEFAULT));
        System.out.println("3 "+client.update(new UpdateRequest(index, id).doc("tag", Arrays.asList(1, 4, 2)), RequestOptions.DEFAULT));
//        bulkRequest.add(new IndexRequest("users").id(id).source(indexMap));
//        bulkRequest.add(new UpdateRequest().upsert("additional", "hello additional").id(id));
//        bulkRequest.add(new UpdateRequest("users", id).doc("tag", Arrays.asList(1, 4, 2)));
//        BulkResponse responses = this.client.bulk(bulkRequest, RequestOptions.DEFAULT);
//        System.out.println(responses);
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        String host = "192.168.2.162";
        int port = 9200;
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, "http")));
        SimpleOperations simpleOperations = new SimpleOperations(client);

        UUID uuid = UUID.randomUUID();

        String index = "users";
        String id = uuid.toString();

        Map<String, Object> indexMap = new HashMap<String, Object>();
        indexMap.put(ID_FIELD, id);
        indexMap.put("age", 18);
        indexMap.put("say", "hello world");
        indexMap.put("name", "xxxx");
        simpleOperations.indexAsync(index, indexMap);

//         XContentBuilder
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.YAML);
        contentBuilder.startObject();
        XContentBuilder child = contentBuilder.field("name", "h");
        child.startObject("address");
        child.field("shi", "beijing");
        child.field("province", "beijing");
        child.endObject();
        contentBuilder.field("age", 1);
        contentBuilder.array("tag", "news", "blog", "wx");
        contentBuilder.endObject();
        IndexRequest indexRequest = new IndexRequest(index).source(contentBuilder);
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response);

        GetRequest request = simpleOperations.get(index, response.getId());
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, new String[]{"name", CLASSIFY_FIELD,}, new String[]{"tag"});
        request = request.fetchSourceContext(fetchSourceContext);
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        System.out.println(getResponse);


        Map<String, Object> updateMap = new HashMap<String, Object>();
        updateMap.put(ID_FIELD, response.getId());
        updateMap.put(CLASSIFY_FIELD, Arrays.asList(1, 2, 3, 4, 5));
        UpdateResponse updateResponse = simpleOperations.update(index, updateMap); // execute partial update
        System.out.println(updateResponse);

        String jsonString = "{\"created\":\"2017-01-01\"}";
        UpdateRequest updateRequest = new UpdateRequest("user", "3").doc(jsonString, XContentType.JSON).upsert(jsonString, XContentType.JSON);
        System.out.println(client.update(updateRequest, RequestOptions.DEFAULT));

        System.out.println("======================bulk api demo ================================");
        simpleOperations.bulkRequest("a");


    }
}
