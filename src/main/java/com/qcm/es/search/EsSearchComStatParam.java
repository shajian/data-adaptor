package com.qcm.es.search;

import com.qcm.entity.EsComStat;
import com.qcm.util.MiscellanyUtil;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
@Getter@Setter
public class EsSearchComStatParam extends EsSearchBaseParam<EsComStat> {
    public EsSearchComStatParam() {
        super();
    }

    /**
     * only used to search oc_name
     */
    @Getter
    private String keyword;
    @Getter
    private Map<String, String> filters;
    @Getter
    private double lat;
    @Getter
    private double lon;

    public void setKeyword(String keyword) {
        setKeyword(keyword, false);
    }

    public void setKeyword(String keyword, boolean highlight) {
        keyword = keyword.trim();
        if (MiscellanyUtil.isBlank(keyword)) return;
        this.keyword = keyword;
        if (highlight)
            setHighlights(new String[] {"oc_name", "oc_name.nlp", "oc_name.crf"});
    }



    private void setCoordinateDistance(double[] values) {
        lat = values[0];
        lon = values[1];
        if (filters == null) filters = new HashMap<>();
        filters.put("coordinate", lat+","+lon+","+values[2]);
    }

    public void setFilter(String field, String value) {
        if (filters == null) filters = new HashMap<>();
        filters.put(field, value);
        if (field == "coordinate") {
            String[] latlon = value.split(",");
            lat = Double.parseDouble(latlon[0]);
            lon = Double.parseDouble(latlon[1]);
        }
    }
}
