package com.qcm.controller;

import com.qcm.api.CompanyGraph;
import com.qcm.api.RedisCompanySearcher;
import com.qcm.es.entity.EsCompanyEntity;
import com.qcm.api.EsCompanySearcher;
import com.qcm.es.search.EsSearchCompanyParam;
import com.qcm.es.repository.EsCompanyRepository;
import com.qcm.entity.EsCompanyTripleMatch;
import com.qcm.graph.GraphParam;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/company")
public class CompanyController {
    @PostMapping("/mget")
    public List<EsCompanyEntity> mget(@RequestBody EsSearchCompanyParam input) {
        return EsCompanyRepository.singleton().mget(input);
    }

    /**
     * get company codes according their full names.
     * this is fine matching
     * @param names array of full names
     * @return All elements are company code. Note that if there are some companies
     *          with the same name, then the code will be multi-codes joined with
     *          separator ','
     */
    @PostMapping("/fname2code")
    public String[] fname2code(@RequestBody String[] names) {
        try {
            return EsCompanySearcher.fname2code(names);
        } catch (Exception e) {
            return RedisCompanySearcher.fname2code(names);
        }
    }

    /**
     * The similar with `fname2code`, the only difference is that the parameters `names` may not
     * be full names, so this match is fuzzy matching.
     * @param names
     * @return
     */
    @PostMapping("/name2code")
    public EsCompanyTripleMatch[] name2code(@RequestBody String[] names, @RequestParam(value = "name") String name) {
        try {
//            if (names.length > 0) {
//                return EsCompanySearcher.name2code(names);
//            }
//            EsCompanyTripleMatch[] ms = { EsCompanySearcher.name2code(name) };
//            return ms;
            return null;
        } catch (Exception e) {
            return new EsCompanyTripleMatch[0];
        }
    }

    @PostMapping("/search")
    public void search(@RequestBody EsSearchCompanyParam input) {

    }

    @PostMapping("/graph")
    public String aggregate(@RequestBody GraphParam param) {
        return CompanyGraph.aggregate(param);
    }
}
