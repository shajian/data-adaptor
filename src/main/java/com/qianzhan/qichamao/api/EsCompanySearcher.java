package com.qianzhan.qichamao.api;

import com.qianzhan.qichamao.dal.es.EsCompanyInput;
import com.qianzhan.qichamao.dal.es.EsCompanyRepository;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.EsCompanyMatch;
import com.qianzhan.qichamao.entity.EsCompanyTripleMatch;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsCompanySearcher {
    /**
     *
     * @param name full/part name of company.
     * @return
     * @throws Exception
     */
    public static EsCompanyTripleMatch name2code(String name) throws Exception {
        EsCompanyRepository es = new EsCompanyRepository();
        EsCompanyInput input = new EsCompanyInput();
        input.setKeyword(name);
        input.setFields("oc_name");
        input.setSize(5);           // get top 5 documents
        input.setSrc_inc("oc_name", "oc_code");
        // no aggregations or highlighting
        SearchResponse resp = es.search(input);
        if (resp.getShardFailures().length > 0)
            throw new Exception(String.format("name2code(name:%s) failed.\n%s", name,
                    resp.getShardFailures()[0].reason()));

        return parse(name, resp);
    }

    public static EsCompanyTripleMatch[] name2code(String[] names) throws Exception {
        EsCompanyRepository es = new EsCompanyRepository();
        EsCompanyTripleMatch[] matches = new EsCompanyTripleMatch[names.length];
        EsCompanyInput[] inputs = new EsCompanyInput[names.length];
        for (int i = 0; i < names.length; ++i) {
            EsCompanyInput input = new EsCompanyInput();
            input.setKeyword(names[i]);
            input.setFields("oc_name");
            input.setSize(5);           // get top 5 documents
            input.setSrc_inc("oc_name", "oc_code");
            inputs[i] = input;
        }
        MultiSearchResponse resp = es.multiSearch(inputs);
        for (int i = 0; i < names.length; ++i) {
            MultiSearchResponse.Item item = resp.getResponses()[i];
            if (item.getFailure() != null) continue;
            SearchResponse r = item.getResponse();
            matches[i] = parse(names[i], r);
        }
        return matches;
    }

    public static String[] fname2code(String[] names) throws Exception {
        EsCompanyInput input = new EsCompanyInput();
        input.setField("oc_name.key");
        input.setIds(names);
        input.setSrc_inc("oc_name", "oc_code");

        SearchResponse resp = new EsCompanyRepository().multiSearch(input);
        if (resp.getShardFailures().length > 0) {
            throw new Exception(String.format("fullNames2codes failed.\n%s",
                    resp.getShardFailures()[0].reason()));
        }
        String[] codes = new String[names.length];
        Map<String, String> map = new HashMap<>();
        for (SearchHit hit : resp.getHits().getHits()) {
            Map<String, Object> m = hit.getSourceAsMap();
            String name = (String) m.get("oc_name");
            if (!MiscellanyUtil.isBlank(name)) {
                String old = map.getOrDefault(name, null);
                if (old != null) map.put(name, String.format("%s,%s", old, m.get("oc_code")));
                else map.put(name, (String) m.get("oc_code"));
            }
        }
        for (int i = 0; i < names.length; ++i) {
            codes[i] = map.get(names[i]);
        }
        return codes;
    }

    private static EsCompanyTripleMatch parse(String name, SearchResponse resp) throws Exception {
        int min = name.length();
        EsCompanyTripleMatch m = new EsCompanyTripleMatch();
        for (SearchHit hit : resp.getHits().getHits()) {
            Map map = hit.getSourceAsMap();
            String dname = (String)map.get("oc_name");


            m.setOc_code((String)map.get("oc_code"));
            m.setOc_area((String)map.get("oc_area"));
            m.setOc_name(dname);
            min = MiscellanyUtil.getEditDistanceSafe(dname, name);
            break;


//            int distance = MiscellanyUtil.getEditDistanceSafe(dname, name);
//            if (distance < min) {
//                m.setOc_code((String)map.get("oc_code"));
//                m.setOc_area((String)map.get("oc_area"));
//                m.setOc_name(dname);
//                min = distance;
//                if (min == 0) break;
//            }

        }
        m.setConfidence(1.0f-((float)min)/name.length());
        return m;
    }


}
