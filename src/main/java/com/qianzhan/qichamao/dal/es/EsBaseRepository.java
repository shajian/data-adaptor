package com.qianzhan.qichamao.dal.es;

import com.qianzhan.qichamao.util.BeanUtil;
import lombok.Getter;
import org.apache.http.util.Asserts;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsBaseRepository<T> {
    protected RestHighLevelClient client;
    protected Class<T> clazz;
    @Getter
    protected EsIndexMeta indexMeta;
    public EsBaseRepository() {

        client = EsClient.getClient();
        ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
        clazz = (Class<T>) type.getActualTypeArguments()[0];
        indexMeta = clazz.getAnnotation(EsIndexMeta.class);
    }

    public void updateIndexSetting() {
        EsIndexSetting setting = new EsIndexSetting().set(indexMeta.index());
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexMeta.index());
        Settings.Builder builder = Settings.builder()
                .put("index.number_of_replicas", setting.getReplicas());
//        Map<String, String> analyzers = setting.getAnalyzers();
//        if (analyzers != null) {
//            for (String k : analyzers.keySet()) {
//                String analyzer_prefix = String.format("analysis.analyzer.%s_analyzer.", k);
//                String tokenizer_prefix = String.format("analysis.tokenizer.%s_tokenizer.", k);
//                builder.put(analyzer_prefix+"tokenizer", String.format("%s_tokenizer", k));
//                String[] vals = analyzers.get(k).split(" ");    // sep=pattern [-\.\|,\s]
//                builder.put(tokenizer_prefix+"type", vals[0]);        // std=standard 1
//                builder.put(tokenizer_prefix+"pattern", vals[1]);
//            }
//        }
        request.settings(builder);
        try {
            AcknowledgedResponse resp = client.indices().putSettings(request, RequestOptions.DEFAULT);
            boolean acknowledged = resp.isAcknowledged();
            if (acknowledged) {
                System.out.println(String.format("update index '%s' setting is acknowledged", indexMeta.index()));
            } else {
                String msg = String.format("update index '%s' setting is not acknowledged", indexMeta.index());
                System.out.println(msg);
                // todo log error
            }
        } catch (IOException e) {

        }
    }
    /**
     * create index and schema-mapping for a given Type
     * @throws Exception
     */
    public void map() throws Exception {
        String index = indexMeta.index();
        EsIndexSetting setting = new EsIndexSetting().set(index);
        CreateIndexRequest request = new CreateIndexRequest(index);

        Settings.Builder setBuilder = Settings.builder()
                .put("index.number_of_shards", setting.getShards())
                .put("index.number_of_replicas", setting.getReplicas());
        Map<String, String> map = setting.getAnalyzers();
        if (map != null) {
            for (String k : map.keySet()) {
                String analyzer_prefix = String.format("analysis.analyzer.%s_analyzer.", k);
//                String tokenizer_prefix = String.format("analysis.tokenizer.%s_tokenizer.", k);
//                setBuilder.put(analyzer_prefix+"tokenizer", String.format("%s_tokenizer", k));
                // analysis.analyzer.custom_uax_url_email.type
                String[] vals = map.get(k).split("\\s");    // sep=pattern [-\.\|,\s]
//                setBuilder.put(tokenizer_prefix+"type", vals[0]);        // std=standard 1
//                setBuilder.put(tokenizer_prefix+"pattern", vals[1]);
                setBuilder.put(analyzer_prefix+"type", vals[0]);
                setBuilder.put(analyzer_prefix+"pattern", vals[1]);
            }
        }
        request.settings(setBuilder);

//        CreateIndexResponse r1 = client.indices().create(request, RequestOptions.DEFAULT);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                Field[] fields = clazz.getDeclaredFields();
                for(Field field : fields) {
                    String name = field.getName();
                    EsFieldMeta m = field.getAnnotation(EsFieldMeta.class);
                    builder.startObject(name);
                    {
                        if (m.type() == EsFieldType.text) {
                            EsAnalyzer[] analyzers = m.analyzers();
                            Asserts.check(analyzers[0] != EsAnalyzer.keyword,
                                    "first analyzer should not be EsAnalyzer.keyword for type `EsFieldType.text`");
                            builder.field("type", m.type().getF_name()).field("analyzer", analyzers[0].name());
                            if (analyzers.length > 1) {
                                builder.startObject("fields");
                                {
                                    for (int i = 1; i < analyzers.length; ++i) {
                                        builder.startObject(analyzers[i].getF_name());
                                        {
                                            if (analyzers[i] == EsAnalyzer.keyword)
                                                builder.field("type", EsFieldType.keyword.getF_name())
                                                        .field("doc_values", m.doc_values());
                                            else
                                                builder.field("type", EsFieldType.text.getF_name())
                                                        .field("analyzer", analyzers[i].name());
                                        }
                                        builder.endObject();
                                    }
                                }
                                builder.endObject();
                            }
                        } else {
                            builder.field("type", m.type().getF_name()).field("doc_values", m.doc_values());
                        }
                    }
                    builder.endObject();
                }

            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping(builder);
        CreateIndexResponse resp = client.indices().create(request, RequestOptions.DEFAULT);
        if (resp.isAcknowledged()) {
            System.out.println("succeed to create index and mapping");
        } else {
            System.out.println("failed to create index and mapping");
        }
    }

    /**
     * index data(some documents) into ES
     * @param docs a list of documents to be indexed
     */
    public void index(List<T> docs) throws Exception {
//        ParameterizedType paramType = (ParameterizedType)docs.getClass().getGenericSuperclass();
//        Class<T> clazz = (Class) paramType.getActualTypeArguments()[0];
        // the same as (Class<T>) list.iterator().next().getClass();
        BulkRequest request = new BulkRequest();

        String id = indexMeta.id();
        String index = indexMeta.index();
        for (T doc :
                docs) {
            Map map = BeanUtil.bean2Map(doc);
            request.add(new IndexRequest(index).id(map.get(id).toString()).source(map, XContentType.JSON));
        }
        BulkResponse resp = client.bulk(request, RequestOptions.DEFAULT);
        for (BulkItemResponse r : resp) {
            if (r.isFailed()) {
                BulkItemResponse.Failure fail = r.getFailure();
                String msg = fail.getMessage();
                String docId = fail.getId();
                String stackTrace = fail.getCause().getMessage();
                // todo: log failure information
            }
        }
    }

    public void index(T doc) {
        try {
            Map map = BeanUtil.bean2Map(doc);
            IndexRequest request = new IndexRequest(indexMeta.index()).id(map.get(indexMeta.id()).toString()).source(map);
            IndexResponse resp = client.index(request, RequestOptions.DEFAULT);

            // todo: log response
            if (resp.getResult() == DocWriteResponse.Result.CREATED) {
                // this doc was created firstly
            } else if (resp.getResult() == DocWriteResponse.Result.UPDATED) {
                // this doc had existed and was updated just now
            } else {
                // some error may be happened
            }
        } catch (Exception e) {

        }
    }

    public void partialUpdate(Map map) {
        String id = (String) map.remove(indexMeta.id());
        if (map.size() > 0) {
            UpdateRequest request = new UpdateRequest(indexMeta.index(), id).doc(map);
            request.docAsUpsert(false); // explicitly turn off upsert
            try {
                UpdateResponse resp = client.update(request, RequestOptions.DEFAULT);
                /**
                 * "_index": "company_2",
                 * "_type": "_doc",
                 * "_version": 2,   # auto incremented by 1 after each updating
                 * "result": "updated",
                 * "_shards": {
                 *      "total": 1, # the total shards related with updated document(s)
                 *      "successful": 1,    # successful shards related with updated document(s)
                 *      "failed": 0         # failed shards related with updated document(s)
                 *  },
                 *  "created": false
                 */
                long version = resp.getVersion();
                if (resp.getResult() == DocWriteResponse.Result.CREATED) {
                    // insert this doc (upsert)
                } else if (resp.getResult() == DocWriteResponse.Result.UPDATED) {
                    // partially update this doc
                } else if (resp.getResult() == DocWriteResponse.Result.NOOP) {
                    // no operation on this doc
                } else if (resp.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    // not found for this doc (can't go here for upsert/index/create)
                }
                ReplicationResponse.ShardInfo shardInfo = resp.getShardInfo();
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    // log that on some shards, parts of document(s) are failed to be updated
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        String reason = failure.reason();   // failure reason for current shard
                    }
                }
            } catch (IOException e) {
                //
            }
        }
    }

    public void partialUpdate(List<Map> maps) {
        BulkRequest request = new BulkRequest();
        boolean flag = true;
        for (Map map : maps) {
            if (map.size() < 2) continue;
            String id = (String) map.remove(indexMeta.id());
            if (id == null) continue;
            flag = false;
            request.add(new UpdateRequest(indexMeta.index(), id).doc(map).docAsUpsert(false));
        }
        if (flag) return;
        try {
            BulkResponse resp = client.bulk(request, RequestOptions.DEFAULT);
        } catch (IOException e) {

        }
    }

    public boolean delete() throws IOException {
        DeleteRequest request = new DeleteRequest(indexMeta.index());
        DeleteResponse resp = client.delete(request, RequestOptions.DEFAULT);
        if (resp.status() == RestStatus.OK) {
            return true;
        }
        return false;
    }

    public void delete(String id) {
        DeleteRequest request = new DeleteRequest(indexMeta.index(), id);
        try {
            DeleteResponse resp = client.delete(request, RequestOptions.DEFAULT);
            long version = resp.getVersion();
            if (resp.getResult() == DocWriteResponse.Result.DELETED) {
                // delete successfully
            } else {
                // delete failed
            }

        } catch (IOException e) {

        }
    }

    public void delete(List<String> ids) {
        BulkRequest request = new BulkRequest();
        for (String id : ids) {
            request.add(new DeleteRequest(indexMeta.index(), id));
        }
        try {
            BulkResponse resp = client.bulk(request, RequestOptions.DEFAULT);
            for (BulkItemResponse r : resp) {
                if (r.isFailed()) {
                    String err = r.getFailureMessage();
                    String id = r.getId();
                    // log error
                } else {
                    DeleteResponse dr = (DeleteResponse) r.getResponse();
                    if (dr.getResult() == DocWriteResponse.Result.DELETED) {
                        // log successfully deleted id
                    } else if (dr.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                        // doc of this id is not found
                    }
                }
            }
        } catch (IOException e) {

        }
    }

    public boolean exists(String id) {
        GetRequest request = new GetRequest(indexMeta.index(), id);
        request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        try {
            return client.exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {

        }
        return false;
    }

    public boolean exists() {
        GetIndexRequest request = new GetIndexRequest(indexMeta.index());
        request.humanReadable(true);
        request.local(false);
        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {

        }
        return false;
    }

    public T get(EsBaseInput<T> input) {
        Asserts.notNull(input.getId(), "must set id for EsQueryInput when call `get` in EsBaseRepository");
        GetRequest request = new GetRequest(indexMeta.index(), input.getId());
        Asserts.check(input.isSrc_flag(), "must turn `src_flag` for class `EsQueryInput` in action 'get'");

        if ((input.getSrc_inc() != null && input.getSrc_inc().length > 0) ||
                (input.getSrc_exc() != null && input.getSrc_exc().length > 0)) {
            String[] includes = input.getSrc_inc() == null ? Strings.EMPTY_ARRAY : input.getSrc_inc();
            String[] excludes = input.getSrc_exc() == null ? Strings.EMPTY_ARRAY : input.getSrc_exc();
            request.fetchSourceContext(new FetchSourceContext(true, includes, excludes));
        }
        try {
            GetResponse resp = client.get(request, RequestOptions.DEFAULT);
            if (resp.isExists()) {
                Map<String, Object> map = resp.getSource();
                return BeanUtil.map2Bean(map, clazz);
            }
        } catch (Exception e) {

        }
        return null;
    }

    public List<T> mget(EsBaseInput<T> input) {
        Asserts.notNull(input.getIds(), "must set ids for EsQueryInput when call `get` in EsBaseRepository");
        MultiGetRequest request = new MultiGetRequest();
        Asserts.check(input.isSrc_flag(), "must turn `src_flag` for class `EsQueryInput` in action 'get'");

        FetchSourceContext ctx = null;
        if ((input.getSrc_inc() != null && input.getSrc_inc().length > 0) ||
                (input.getSrc_exc() != null && input.getSrc_exc().length > 0)) {
            String[] includes = input.getSrc_inc() == null ? Strings.EMPTY_ARRAY : input.getSrc_inc();
            String[] excludes = input.getSrc_exc() == null ? Strings.EMPTY_ARRAY : input.getSrc_exc();
            ctx = new FetchSourceContext(true, includes, excludes);
        }
        String index = indexMeta.index();
        for (String id : input.getIds()) {;
            MultiGetRequest.Item item = new MultiGetRequest.Item(index, id);
            if (ctx != null)
                item.fetchSourceContext(ctx);
            request.add(item);
        }
        List<T> list = new ArrayList<T>();
        try {
            MultiGetResponse resp = client.mget(request, RequestOptions.DEFAULT);
            for (MultiGetItemResponse r : resp.getResponses()) {
                GetResponse gr = r.getResponse();
                if (gr.isExists()) {
                    Map<String, Object> map = gr.getSource();
                    list.add(BeanUtil.map2Bean(map, clazz));
                }
            }
        } catch (IOException e) {
            // log exception
        }
        return list;
    }


    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {

            }
        }
    }
}
