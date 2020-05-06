package com.qianzhan.qichamao.controller;

import com.qianzhan.qichamao.api.EsCompanySearcher;
import com.qianzhan.qichamao.api.RedisCompanySearcher;
import com.qianzhan.qichamao.dal.es.EsCompanyInput;
import com.qianzhan.qichamao.dal.es.EsCompanyRepository;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.EsCompanyTripleMatch;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/company")
public class CompanyController {
    @PostMapping("/mget")
    public List<EsCompany> mget(@RequestBody EsCompanyInput input) {
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
    public void search(@RequestBody EsCompanyInput input) {

    }
}
