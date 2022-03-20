package com.feifei.es;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @Description:
 * @ClassName: ESBase
 * @Author chengfei
 * @DateTime 2021/4/11 13:16
 **/
public class ESBase {

    private static Log logger = LogFactory.getLog(ESBase.class);

    /**
     * @param client    客户端
     * @param indexName 索引的名称
     * @Todo: 创建索引
     * @return:
     * @DateTime: 2021/4/11 14:05
     */
    void createIndex(RestHighLevelClient client, String indexName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * @param client
     * @Todo: 得到集群所有的index列表
     * @return:
     * @DateTime: 2021/4/11 14:18
     */
    void getindex(RestHighLevelClient client) throws IOException {
        GetIndexRequest request = new GetIndexRequest("prodect*");
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        String[] indices = response.getIndices();
        for (String index : indices) {
            System.out.println(index);
        }
    }


    /**
     * @param client
     * @Todo: 删除对应的index
     * @return:
     * @DateTime: 2021/4/11 14:21
     */
    void deleteIndex(RestHighLevelClient client) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("aa");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
    }

    /**
     * @param
     * @Todo: 单条数据的插入
     * @return:
     * @DateTime: 2021/4/11 14:46
     */
    @Test
    public void insertData() throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("node03", 9200))
        );
        IndexRequest request = new IndexRequest("personas_wl_user");

        HashMap<String, String> map = new HashMap<>();
//        map.put("name", "pingguo");
//        map.put("desc", "pingguo11,gei xuxu mai d");
//        map.put("price", "4999");
//        map.put("tags", "pingguo");

        map.put("app_version", "1.2.5");
        map.put("city", "大上海");
        request.id("110");
        request.source(map);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }


    /**
     * @Todo: 批量插入数据
     * @return:
     * @DateTime: 2021/4/11 14:49
     */
    @Test
    public void batchInsertData() throws IOException, InterruptedException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("node03", 9200))
        );

        //准备数据
//        BulkRequest request = new BulkRequest("product");
        ProductEntity product0 = new ProductEntity("pingguo111", "pingguo zhen d henhao", 3900, "henhao fashou");
        ProductEntity product2 = new ProductEntity("pingguo erji111", "pingguo zhen d henhao", 3800, "henhao fashou");
        ProductEntity product3 = new ProductEntity("pingguo erji11", "pingguo zhen d henhao", 3700, "henhao fashou");
        ProductEntity product4 = new ProductEntity("pingguo pingban11", "pingguo zhen d henhao", 3600, "henhao fashou");
        ProductEntity product1 = new ProductEntity("pingguo pingban11", "pingguo zhen d henhao", 3500, "henhao fashou");
        Gson gson = new Gson();


        //BulkProcessor 的init操作
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.info("id: " + executionId + " req: " + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                logger.info("id: " + executionId + " bulk success!");

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {
                logger.error("id: " + executionId + "  req: " + request + "  cause: " + failure.getMessage());
                System.exit(110);
            }
        };

        //java8的拉姆达表达式
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (request, bulkListener) ->
                client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
//        builder.setBulkActions(1000);
//        builder.setBulkSize(new ByteSizeValue(5L, ByteSizeUnit.MB));
//        builder.setConcurrentRequests(2);
//        builder.setFlushInterval(TimeValue.timeValueSeconds(30L));
//        builder.setBackoffPolicy(BackoffPolicy
//                .constantBackoff(TimeValue.timeValueSeconds(5L), 3));
        final BulkProcessor bulkProcessor = builder.build();

//        final IndexRequest product = new IndexRequest("product");
//        product.source(gson.toJson(product0));

//        bulkProcessor.add(new IndexRequest("product").source(gson.toJson(product0), XContentType.JSON));
//        bulkProcessor.add(new IndexRequest("product").source(gson.toJson(product1), XContentType.JSON));
//        bulkProcessor.add(new IndexRequest("product").source(gson.toJson(product2), XContentType.JSON));
//        bulkProcessor.add(new IndexRequest("product").source(gson.toJson(product4), XContentType.JSON));
//        bulkProcessor.add(new IndexRequest("product").source(gson.toJson(product3), XContentType.JSON));
        ProductEntity product11 = new ProductEntity("pingguo pingban11", "pingguo zhen d henhao", 3500, "henhao fashou");

        final JSONObject updateJson = new JSONObject();
        updateJson.put("name", "feifei");
        updateJson.put("desc", "feifei牌手机");
        updateJson.put("price", "36501");
        updateJson.put("tags", "hahahahahahahahah");
//        val updateRequest = new UpdateRequest(AppProperties.get("es.user.profile.index"), unique_id).doc(updateJson.toJSONString, XContentType.JSON)
        bulkProcessor.add(new UpdateRequest("product", "8").doc(updateJson.toJSONString(), XContentType.JSON));
//        bulkProcessor.add(new UpdateRequest("product","120").doc(gson.toJson(product1), XContentType.JSON));
//        bulkProcessor.add(new UpdateRequest("product","130").doc(gson.toJson(product2), XContentType.JSON));
//        bulkProcessor.add(new UpdateRequest("product","140").doc(gson.toJson(product3), XContentType.JSON));
//        bulkProcessor.add(new UpdateRequest("product","150").doc(gson.toJson(product4), XContentType.JSON));


        bulkProcessor.flush();
//        bulkProcessor.close();
        Thread.sleep(100);
        client.close();
    }


    @Test
    public void batchInsertBulkData() throws InterruptedException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("node03", 9200))
        );

        final BulkProcessor bulkProcessor = this.bulkProcessor("", client);


        HashMap<String, Object> map = new HashMap<String, Object>() {{
            put("name", "feifei");
            put("desc", "feifei牌手机");
            put("price", "36501");
            put("tags", "hahahahahahahahah");
        }};
        bulkProcessor.add(new IndexRequest("personas_wl_user").source(map));
        bulkProcessor.flush();
        //刷新，插入都是移异步的操作，会导致主程序结束了，数据还没有操作，就会导致插入失败，所有就得休眠一下
        Thread.sleep(1000*2);


    }

    @Test
    public void batchUpdateBulkData() throws InterruptedException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("node03", 9200))
        );

        final BulkProcessor bulkProcessor = this.bulkProcessor("", client);


        HashMap<String, Object> map = new HashMap<String, Object>() {{
            put("name", "feifei-***新增");
            put("desc", "feifei牌手机");
            put("price", "36501");
            put("tags", "hahahahahahahahah");
        }};
        JSONObject jsonObject = new JSONObject(map);
        bulkProcessor.add(new UpdateRequest("product", "8").doc(jsonObject.toJSONString(), XContentType.JSON));
        bulkProcessor.add(new UpdateRequest("product", "9").doc(jsonObject.toJSONString(), XContentType.JSON));
        bulkProcessor.add(new UpdateRequest("product", "10").doc(jsonObject.toJSONString(), XContentType.JSON));

        bulkProcessor.flush();
        //刷新，插入都是移异步的操作，会导致主程序结束了，数据还没有操作，就会导致插入失败，所有就得休眠一下
        Thread.sleep(1000*2);


    }

    /*
     * 创建bulkProcessor
     * */
    public BulkProcessor bulkProcessor(String taskTypeLog, RestHighLevelClient client) {

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        return BulkProcessor.builder(bulkConsumer, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                //在这儿你可以自定义执行同步之前执行什么
            }

            @SneakyThrows
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                //在这儿你可以自定义执行完同步之后执行什么
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                //写入失败后
                logger.error("ES写入失败", failure);
            }
        }).setBulkActions(10) //  达到刷新的条数
                .setFlushInterval(TimeValue.timeValueSeconds(10)) // 固定刷新的时间频率
                .build();
    }

    /**
     * @param client
     * @Todo: 根据id去查询数据的指定字段
     * @return:
     * @DateTime: 2021/4/11 15:28
     */
    void getById(RestHighLevelClient client) throws IOException {
        GetRequest request = new GetRequest("product", "1");

        String[] includes = {"name", "price"};
        String[] excludes = {"desc"};

        request.fetchSourceContext(new FetchSourceContext(true, includes, excludes));

        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        System.out.println(response);
    }

    /**
     * @param client
     * @Todo: 根据id删除数据
     * @return:
     * @DateTime: 2021/4/11 15:39
     */
    void deleteById(RestHighLevelClient client) throws IOException {
        DeleteRequest request = new DeleteRequest("product", "1");

        client.delete(request, RequestOptions.DEFAULT);
    }


    @Test
    public void multGetById() throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("node03", 9200))
        );

        MultiGetRequest request = new MultiGetRequest();
        request.add("product", "2");
//        request.add("personas_wl_user","2");

        request.add("personas_wl_user", "205_697:1530027764136.yoka");

//        request.add(new MultiGetRequest.Item("personas_wl_user","205_697:1530027764136.yoka"));
        request.add(new MultiGetRequest.Item("product", "3"));
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);

        for (MultiGetItemResponse itemResponse : response) {
            System.out.println(itemResponse.getResponse());
            System.out.println(itemResponse.getResponse().getSourceAsString());
            System.out.println();
        }


        client.close();
    }

    @Test
    public void updateByQuery() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("node03", 9200))
        );

        UpdateByQueryRequest request = new UpdateByQueryRequest("personas_wl_user");
        //默认情况下，版本冲突会中止 UpdateByQueryRequest 进程，但是你可以用以下命令来代替
        //版本冲突的时候继续
//        request.setConflicts("proceed");
//        request.setQuery(QueryBuilders.termQuery("_id","205_697:1530027764136.yoka"));
        request.setQuery(QueryBuilders.termQuery("app_version", "sfswegw.pingguo"));
//        request.setQuery(QueryBuilders.termQuery("_id","205_697:1529233539620.ios"));
        //限制更新条数
//        request.setMaxDocs(10);
//        String idOrCode = "ctx._source.user_level_in_hour='2';ctx._source.level_time='1530027900000';";
        String idOrCode = "ctx._source.city='*********来自大Luca**********';ctx._source.province='----------哈哈哈------';";
        request.setScript(new Script(idOrCode));
//        request.setScript(new Script(ScriptType.INLINE,"painless","ctx._source.desc='"+str+"';", Collections.emptyMap()));

        BulkByScrollResponse response = client.updateByQuery(request, RequestOptions.DEFAULT);
        System.out.println(response);


        client.close();
    }

}
