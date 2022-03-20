package com.feifei.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @Description:
 * @ClassName: APPMain
 * @Author chengfei
 * @DateTime 2021/4/11 14:04
 **/
public class APPMain {
    public static void main(String[] args) throws IOException {
        //创建客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("node03", 9200)
                        , new HttpHost("node03", 9201)));
        ESBase esBase = new ESBase();
//        esBase.createIndex(client,"aa");

//        esBase.insertData(client);

//        esBase.batchInsertData(client);

        esBase.getById(client);

        client.close();
    }
}
