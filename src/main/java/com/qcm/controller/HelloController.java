package com.qcm.controller;

import com.qcm.es.search.EsSearchCompanyParam;
import com.qcm.entity.MongoComDtl;
import com.qcm.entity.RetrieveRange;
import org.springframework.web.bind.annotation.*;

@RestController
public class HelloController {
    @RequestMapping("/")
    public String index() {
        return "welcome to `data-adaptor`~";
    }

    @GetMapping("/hello")
    public String hello(@RequestParam(value = "name", defaultValue = "World") String name) {
        return String.format("Hello %s!", name);
    }

    @GetMapping("/range")
    public RetrieveRange range(@RequestParam(value = "start", defaultValue = "1") int start,
                               @RequestParam(value = "count", defaultValue = "1") int count) {
        return new RetrieveRange(){{setStart(start);setCount(count);}};
    }

    @PostMapping("/company-detail")
    public MongoComDtl company_detail(@RequestParam(value = "code") String code) {
        return null;
    }

    @PostMapping("/company-search")
    public void company_search(@RequestBody EsSearchCompanyParam input) {

    }
}
