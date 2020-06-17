package com.qianzhan.qichamao.api;

import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.es.EsSearchCompanyParam;
import com.qianzhan.qichamao.es.EsCompanyRepository;
import com.qianzhan.qichamao.entity.EsCompanyTripleMatch;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsCompanySearcher {
    /**
     *
     * @param name full/part name of company.
     * @return the first document
     * @throws Exception
     */
    public static List<EsCompanyTripleMatch> name2code(String name) throws Exception {
        EsCompanyRepository es = EsCompanyRepository.singleton();
        EsSearchCompanyParam input = new EsSearchCompanyParam();
        input.setKeyword(name);
        input.setSortField("establish_date");
        input.setFields("oc_name");
        input.setSize(5);           // get top 5 documents
        input.setSrc_inc("oc_name", "oc_code", "oc_area", "oc_status");
        // no aggregations or highlighting
        SearchResponse resp = es.search(input);
        if (resp.getShardFailures().length > 0)
            throw new Exception(String.format("name2code(name:%s) failed.\n%s", name,
                    resp.getShardFailures()[0].reason()));

        return parse(name, resp);
    }

    public static List<EsCompanyTripleMatch>[] name2code(String[] names) throws Exception {
        EsCompanyRepository es = EsCompanyRepository.singleton();
        List<EsCompanyTripleMatch>[] matches = new List[names.length];
        EsSearchCompanyParam[] inputs = new EsSearchCompanyParam[names.length];
        for (int i = 0; i < names.length; ++i) {
            EsSearchCompanyParam input = new EsSearchCompanyParam();
            input.setKeyword(names[i]);
            input.setFields("oc_name");
            input.setSize(5);           // get top 5 documents
            input.setSrc_inc("oc_name", "oc_code", "oc_area");
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
        EsSearchCompanyParam input = new EsSearchCompanyParam();
        input.setField("oc_name.key");
        input.setIds(names);
        input.setSrc_inc("oc_name", "oc_code", "oc_area");

        SearchResponse resp = EsCompanyRepository.singleton().multiSearch(input);
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

    private static List<EsCompanyTripleMatch> parse(String name, SearchResponse resp) throws Exception {
        int min = name.length();
        EsCompanyTripleMatch m = null;
        List<EsCompanyTripleMatch> matches = new ArrayList<>();
        for (SearchHit hit : resp.getHits().getHits()) {
            Map map = hit.getSourceAsMap();
            Number oc_status = (Number) map.get("oc_status");
            String dname = (String)map.get("oc_name");

            EsCompanyTripleMatch match = new EsCompanyTripleMatch();
            match.setOc_code((String)map.get("oc_code"));
            match.setOc_area((String)map.get("oc_area"));
            match.setOc_name(dname);
            if (oc_status != null) {
                match.setOc_status(oc_status.byteValue());
            }
            int dist = MiscellanyUtil.getEditDistanceSafe(dname, name);
            if (dist == 0) {
                match.setConfidence(1.0);
                matches.add(match);
            } else if (matches.size() > 0) {
                continue;
            } else if (dist < min) {
                min = dist;
                m = match;
                m.setConfidence(1.0f-((float)dist)/name.length());
            }
        }
        if (matches.size() == 0 && m != null) {
            matches.add(m);
        }
        return matches;
    }


}
