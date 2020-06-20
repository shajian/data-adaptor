package com.qianzhan.qichamao.es;

import com.qianzhan.qichamao.util.MiscellanyUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.io.IOException;
import java.util.List;

public class EsUpdateLogRepository extends EsBaseRepository<EsUpdateLogEntity> {
    private static EsUpdateLogRepository _instance = new EsUpdateLogRepository();
    public static EsUpdateLogRepository singleton() { return _instance; }

    public SearchResponse search(EsUpdateLogSearchParam param) throws IOException {
        SearchRequest request = new SearchRequest(indexMeta.index());
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(param.getSize());
        builder.from(param.getFrom());
        builder.sort(new FieldSortBuilder(param.getSortField()).order(param.getSortOrder()));
        builder.query(query(param));
        return client.search(request.source(builder), RequestOptions.DEFAULT);
    }

    private QueryBuilder query(EsUpdateLogSearchParam input) {
        QueryBuilder builder = QueryBuilders.boolQuery().filter(filter(input)).must(must(input));
        return builder;
    }

    private QueryBuilder must(EsUpdateLogSearchParam input) {
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        return bool;
    }

    private QueryBuilder filter(EsUpdateLogSearchParam input) {
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        if (input.getFilters() == null) return bool;
        for (String field : input.getFilters().keySet()) {
            List<String> values = input.getFilters().get(field);
            switch (field) {
                case "table_name":
                case "field_names":
                case "task_name":
                    BoolQueryBuilder term = QueryBuilders.boolQuery();
                    for (String value : values) {
                        term.should(QueryBuilders.termQuery(field, value));
                    }
                    bool.must(term);
                    break;
                case "create_time":
                case "last_read_time":
                    BoolQueryBuilder date = QueryBuilders.boolQuery();
                    for (String value : values) {
                        String[] segs = value.split("|");
                        date.should(QueryBuilders.rangeQuery(field).gte(segs[0]).lt(segs[1]));
                    }
                    bool.must(date);
                    break;
                case "tbl_id":
                    BoolQueryBuilder range = QueryBuilders.boolQuery();
                    for (String value : values) {
                        String[] segs = value.split("-");
                        if (segs.length == 2) {
                            if (segs[0].length() > 0 && segs[1].length() > 0) {
                                range.should(QueryBuilders.rangeQuery(field).gte(segs[0]).lt(segs[1]));
                            } else if (segs[0].length() > 0) {
                                range.should(QueryBuilders.rangeQuery(field).gte(segs[0]));
                            } else if (segs[1].length() > 0) {
                                range.should(QueryBuilders.rangeQuery(field).lt(segs[1]));
                            }
                        }
                    }
                    bool.must(range);
                    break;
            }
        }
        return bool;
    }
}
