package com.qianzhan.qichamao.es.search;

import com.qianzhan.qichamao.es.EsUpdateLogEntity;
import com.qianzhan.qichamao.es.EsUpdateLogRepository;
import com.qianzhan.qichamao.es.EsUpdateLogSearchParam;
import com.qianzhan.qichamao.task.com.TaskType;
import com.qianzhan.qichamao.util.BeanUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsUpdateLogSearcher {
    public static EsUpdateLogEntity getLastLog(TaskType task) throws IOException {
        EsUpdateLogSearchParam param = new EsUpdateLogSearchParam();
        param.setSortField("tbl_id");
        param.setSortOrder(SortOrder.DESC);
        param.setFilter("task_name", task.name());
        param.setSize(1);
        SearchResponse resp = EsUpdateLogRepository.singleton().search(param);
        for (SearchHit hit : resp.getHits().getHits()) {
            Map map = hit.getSourceAsMap();
            EsUpdateLogEntity log = BeanUtil.map2Bean(map, EsUpdateLogEntity.class);
            return log;
        }
        return null;
    }
}
