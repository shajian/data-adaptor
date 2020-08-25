package com.qcm.es.search;

import com.qcm.es.entity.EsUpdateLogEntity;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public class EsUpdateLogSearchParam extends EsSearchBaseParam<EsUpdateLogEntity> {
    private Map<String, List<String>> filters;

    public void setFilter(String field, String value) {
        if (filters == null) filters = new HashMap<>();
        List<String> values = filters.get(field);
        if (values == null) {
            values = new ArrayList<>();
            filters.put(field, values);
        }
        values.add(value);
    }


}
