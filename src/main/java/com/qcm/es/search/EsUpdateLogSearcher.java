package com.qcm.es.search;

import com.qcm.es.entity.EsUpdateLogEntity;
import com.qcm.es.repository.EsUpdateLogRepository;
import com.qcm.task.maintask.TaskType;
import com.qcm.util.BeanUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
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
