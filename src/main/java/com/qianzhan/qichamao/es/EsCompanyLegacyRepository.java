package com.qianzhan.qichamao.es;

public class EsCompanyLegacyRepository extends EsBaseRepository<EsCompanyEntityLegacy> {

    private static EsCompanyLegacyRepository repository;
    public static EsCompanyLegacyRepository singleton() {
        if (repository == null) {
            repository = new EsCompanyLegacyRepository();
        }
        return repository;
    }


}
