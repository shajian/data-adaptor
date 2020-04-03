package com.qianzhan.qichamao.dal.es;

import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.unit.TimeValue;
//import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class EsCompanyRepository extends EsBaseRepository<EsCompany> {
    public EsCompanyRepository() {
        super();
    }

    /**
     * search api
     */

    public SearchResponse search(EsCompanyInput input) throws IOException {
        if (input.getAccessMode() == 2 && !MiscellanyUtil.isBlank(input.getScrollId())) {   // next scroll step
            SearchScrollRequest request = new SearchScrollRequest(input.getScrollId());
            request.scroll(TimeValue.timeValueSeconds(30));
            return client.scroll(request, RequestOptions.DEFAULT);
        }
        SearchRequest request = new SearchRequest(indexMeta.index());
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(input.getSize());
        if (input.getAccessMode() == 0) builder.from(input.getPage()*input.getSize());
        if (input.getTimeOut() > 0) {
            builder.timeout(new TimeValue(input.getTimeOut(), TimeUnit.SECONDS));
        }
        if (input.isSrc_flag()) {
            if (!MiscellanyUtil.isArrayEmpty(input.getSrc_inc()) ||
                    !MiscellanyUtil.isArrayEmpty(input.getSrc_exc())) {
                String[] includes = input.getSrc_inc() == null ? Strings.EMPTY_ARRAY : input.getSrc_inc();
                String[] excludes = input.getSrc_exc() == null ? Strings.EMPTY_ARRAY : input.getSrc_exc();
                builder.fetchSource(includes, excludes);
            }
        } else {
            builder.fetchSource(false);
        }

        if ("coordinate".equals(input.getSortField())) {
            builder.sort(new GeoDistanceSortBuilder("coordinate", input.getLat(), input.getLon()).order(input.getSortOrder()));
        } else if ("_score".equals(input.getSortField())) {
            builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        } else {
            builder.sort(new FieldSortBuilder(input.getSortField()).order(input.getSortOrder()));
        }
        if (input.getAccessMode() == 1) {   // search after
            builder.sort(new FieldSortBuilder("oc_code").order(SortOrder.ASC));
        }

        if (!MiscellanyUtil.isArrayEmpty(input.getHighlights()) && input.getAccessMode() != 2) {
            HighlightBuilder hlBuilder = new HighlightBuilder();
            hlBuilder.preTags(input.getHighlightTags()[0]).postTags(input.getHighlightTags()[1]);
            for (String field : input.getHighlights()) {
                hlBuilder.field(field);
            }
            builder.highlighter(hlBuilder);
        }
        if (input.getAggs() != null && input.getAccessMode() != 2 && input.getPage() == 0) {
            EsAggSetting aggs = input.getAggs();
            aggs.shrink(input.getFilters());
            if (aggs.getTerms() != null) {
                for (String field : aggs.getTerms()) {
                    TermsAggregationBuilder term = AggregationBuilders.terms(field).field(field);
                    builder.aggregation(term);
                }
            }
            if (aggs.getRanges() != null) {
                for (String field : aggs.getRanges().keySet()) {
                    int[] sentinels = aggs.getRanges().get(field);
                    RangeAggregationBuilder range = AggregationBuilders.range(field)
                            .addUnboundedTo(sentinels[0])
                            .addUnboundedFrom(sentinels[sentinels.length-1]);
                    for (int i = 0; i < sentinels.length -1; ++i) {
                        range.addRange(sentinels[i], sentinels[i+1]);
                    }
                    builder.aggregation(range);
                }
            }
            if (aggs.getGeos() != null) {
                for (String field : aggs.getGeos().keySet()) {
                    double[] values = aggs.getGeos().get(field);
                    GeoDistanceAggregationBuilder geo = AggregationBuilders
                            .geoDistance(field, new GeoPoint(input.getLat(), input.getLon()))
                            .addUnboundedTo(values[2])
                            .addUnboundedFrom(values[values.length-1]);
                    for (int i = 0; i < values.length - 1; ++i) {
                        geo.addRange(values[i], values[i+1]);
                    }
                    builder.aggregation(geo);
                }
            }
            if (aggs.getDateHistograms() != null) {
                for (String field : aggs.getDateHistograms().keySet()) {
                    EsAggSetting.DateHistogramSetting dh = aggs.getDateHistograms().get(field);
                    if (dh.getMax()!=null || dh.getMin()!=null) {
                        RangeQueryBuilder rangeBuilder = QueryBuilders.rangeQuery(field);
                        if (dh.getMin() != null) {
                            rangeBuilder.gte(dh.getMin());
                        }
                        if (dh.getMax() != null) {
                            rangeBuilder.lt(dh.getMin());
                        }
                        FilterAggregationBuilder filter = AggregationBuilders.filter(field, rangeBuilder)
                                .subAggregation(AggregationBuilders.dateHistogram(field)
                                        .dateHistogramInterval(dh.getInterval()));
                        builder.aggregation(filter);
                    }
                    else {
                        DateHistogramAggregationBuilder date = AggregationBuilders.dateHistogram(field)
                                .dateHistogramInterval(dh.getInterval());
                        builder.aggregation(date);

                    }
                }
            }
            if (aggs.getFilters() != null) {
                for (String field : aggs.getFilters().keySet()) {
                    String[] values = aggs.getFilters().get(field);
                    FiltersAggregator.KeyedFilter[] keyedFilters = new FiltersAggregator.KeyedFilter[values.length];
                    for (int i = 0; i < values.length; ++i) {
                        keyedFilters[i] = new FiltersAggregator.KeyedFilter(values[i], QueryBuilders.termQuery(field, values[i]));
                    }
                    FiltersAggregationBuilder filters = AggregationBuilders.filters(field, keyedFilters);
                    builder.aggregation(filters);
                }
            }
        }

        builder.query(query(input));
        if (input.getAccessMode() == 2) {
            request.scroll(TimeValue.timeValueMinutes(1));
        } else if (input.getAccessMode() == 1 && !MiscellanyUtil.isArrayEmpty(input.getSearchAfter())) {
            builder.searchAfter(input.getSearchAfter());
        }
        return client.search(request.source(builder), RequestOptions.DEFAULT);
    }

    private QueryBuilder query(EsCompanyInput input) {
        QueryBuilder builder = QueryBuilders.boolQuery().filter(filter(input)).must(must(input));
        if ("_score".equals(input.getSortField())) {     // sort by score
            if (MiscellanyUtil.isArrayEmpty(input.getFields())) {   // tags influence
                FunctionScoreQueryBuilder.FilterFunctionBuilder[] builders =
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[4];
                for (int i = 1; i < 4; ++i) {
                    builders[i-1] = new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                            new BoolQueryBuilder().filter(new TermQueryBuilder("tag_"+i, input.getKeyword())),
                            ScoreFunctionBuilders.fieldValueFactorFunction("score_"+i).factor(1000).missing(1)
                    );
                }
                builders[3] = new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.matchAllQuery(),
                        ScoreFunctionBuilders.fieldValueFactorFunction("score").missing(1)
                );
                return QueryBuilders.functionScoreQuery(builder, builders)
                        .scoreMode(FunctionScoreQuery.ScoreMode.FIRST)
                        .boostMode(CombineFunction.MULTIPLY);
            } else {
                return QueryBuilders.functionScoreQuery(builder,
                        ScoreFunctionBuilders.fieldValueFactorFunction("score").missing(1))
                        .boostMode(CombineFunction.MULTIPLY);
            }
        }
        return builder;
    }

    private QueryBuilder must(EsCompanyInput input) {
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        if (input.getMulti_mode() > 0) {
            String field = input.getFields().iterator().next();
            if (input.getSep_type() == 2) {     // intersection
                if (EsCompanyInput.getDef_simple_fields().contains(field)) {
                    for (String key : input.getKeywords().keySet()) {
                        int sign = input.getKeywords().get(key);
                        if (sign == 1)
                            bool.must(QueryBuilders.termQuery(field, key));
                        else
                            bool.mustNot(QueryBuilders.termQuery(field, key));
                    }
                } else {
                    for (String key : input.getKeywords().keySet()) {
                        int sign = input.getKeywords().get(key);
                        if (sign == 1) {
                            if ("business".equals(field) || "oc_address".equals(field)) {
                                bool.must(QueryBuilders.boolQuery()
                                        .should(QueryBuilders.matchPhraseQuery(field, key))
                                        .should(QueryBuilders.matchPhraseQuery(field+".nlp", key).boost(5)));
                            } else if ("oc_name".equals(field)) {

                            }
                        } else {
                            if ("business".equals(field) || "oc_address".equals(field)) {
                                bool.mustNot(QueryBuilders.boolQuery()
                                        .should(QueryBuilders.matchPhraseQuery(field, key))
                                        .should(QueryBuilders.matchPhraseQuery(field+".nlp", key).boost(5)));
                            } else if ("oc_name".equals(field)) {

                            }
                        }
                    }
                }
            } else {                            // union
                for (String key : input.getKeywords().keySet()) {
                    if (EsCompanyInput.getDef_simple_fields().contains(field)) {
                        bool.should(QueryBuilders.termQuery(field, key));
                    } else {
                        if ("business".equals(field) || "oc_address".equals(field)) {
                            bool.should(QueryBuilders.boolQuery()
                                    .should(QueryBuilders.matchPhraseQuery(field, key))
                                    .should(QueryBuilders.matchPhraseQuery(field+".nlp", key).boost(5)));
                        } else if ("oc_name".equals(field)) {

                        }
                    }
                }
            }
            return bool;
        }
        // single mode
        if (!MiscellanyUtil.isArrayEmpty(input.getFields())) {      // union
            for (String field : input.getFields()) {
                if (EsCompanyInput.getDef_simple_fields().contains(field)) {
                    bool.should(QueryBuilders.termQuery(field, input.getKeyword()));
                } else {
                    bool.should(QueryBuilders.matchPhraseQuery(field, input.getKeyword()))
                    .should(QueryBuilders.matchPhraseQuery(field+".nlp", input.getKeyword()).boost(5));
                    if (field.equals("oc_name")) {

                    }
                }
            }
            return bool;
        }
        // generic

        for (String field : EsCompanyInput.getDef_simple_fields()) {
            bool.should(QueryBuilders.termQuery(field, input.getKeyword()));
        }
        bool.should(QueryBuilders.termQuery("tags", input.getKeyword().toUpperCase()));
        bool.should(QueryBuilders.matchPhraseQuery("oc_address", input.getKeyword()).slop(5))
                .should(QueryBuilders.matchPhraseQuery("oc_address.nlp", input.getKeyword()).slop(3).boost(5))
                .should(QueryBuilders.matchPhraseQuery("business", input.getKeyword()).slop(20))
                .should(QueryBuilders.matchPhraseQuery("business.nlp", input.getKeyword()).slop(10).boost(5))
                .should(QueryBuilders.matchPhraseQuery("oc_name", input.getKeyword()).slop(5).boost(20))
                .should(QueryBuilders.matchPhraseQuery("oc_name.nlp", input.getKeyword()).slop(3).boost(30))
                .should(QueryBuilders.matchPhraseQuery("oc_name.crf", input.getKeyword()).slop(3).boost(40))
                .should(QueryBuilders.termQuery("oc_name.key", input.getKeyword()))
                .should(QueryBuilders.termQuery("old_names", input.getKeyword()))
                .should(QueryBuilders.matchPhraseQuery("oc_brands", input.getKeyword()).boost(2))
                .should(QueryBuilders.matchPhraseQuery("oc_brands.nlp", input.getKeyword()).boost(10))
                .should(QueryBuilders.termQuery("oc_brands.key", input.getKeyword()));
        return bool;
    }

    private QueryBuilder filter(EsCompanyInput input) {
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        for (String field : input.getFilters().keySet()) {
            String[] values = input.getFilters().get(field);
            switch (field) {
                case "oc_status":
                case "oc_type":
                case "gb_cats":
                    BoolQueryBuilder term = QueryBuilders.boolQuery();
                    for (String value : values) {
                        term.should(QueryBuilders.termQuery(field, value));
                    }
                    bool.must(term);
                    break;
                case "oc_area":
                case "gb_codes":
                    BoolQueryBuilder std = QueryBuilders.boolQuery();
                    for (String value : values)
                        std.should(QueryBuilders.prefixQuery(field, value));
                    bool.must(std);
                    break;
                case "register_money":
                    BoolQueryBuilder money = QueryBuilders.boolQuery();
                    for (String value : values) {
                        String[] segs = value.split("-");
                        if (segs.length == 2) {
                            if (segs[0].length() > 0 && segs[1].length() > 0) {
                                money.should(QueryBuilders.rangeQuery(field).gte(segs[0]).lt(segs[1]));
                            } else if (segs[0].length() > 0) {
                                money.should(QueryBuilders.rangeQuery(field).gte(segs[0]));
                            } else if (segs[1].length() > 0) {
                                money.should(QueryBuilders.rangeQuery(field).lt(segs[1]));
                            }
                        }
                    }
                    bool.must(money);
                    break;
                case "establish_date":
                    BoolQueryBuilder date = QueryBuilders.boolQuery();
                    for (String value : values) {
                        int year = Integer.parseInt(value);
                        int next_year = year+1;
                        date.should(QueryBuilders.rangeQuery(field).gte(year+"-01-01").lt(next_year+"-01-01"));
                    }
                    bool.must(date);
                    break;
                case "mobile_phones":
                case "fix_phones":
                case "oc_mails":
                    QueryBuilder exist = QueryBuilders.existsQuery(field);
                    bool.must(exist);
                    break;
                case "coordinate":
                    if (values.length == 3) {
                        double lat = Double.parseDouble(values[0]);
                        double lon = Double.parseDouble(values[1]);
                        GeoDistanceQueryBuilder geo = QueryBuilders.geoDistanceQuery(field)
                                .distance(values[2]+"km").point(lat, lon);
                        bool.must(geo);
                    }
                    break;
            }
        }
        return bool;
    }

    /**
     * For simplicity, no aggregation or highlighting is supported.
     * Although it implements searching as the same with `search`, you should
     *  avoid to using this method for too complex searching request. Benefit from
     *  its high efficiency, simple searching(multi-term/match_phrase) are suggested.
     * @param inputs
     * @throws IOException
     */
    public MultiSearchResponse multiSearch(EsCompanyInput[] inputs) throws IOException {
        MultiSearchRequest request = new MultiSearchRequest();
        for (EsCompanyInput input : inputs) {
            SearchRequest r = new SearchRequest(indexMeta.index());
            SearchSourceBuilder b = new SearchSourceBuilder();
            b.from(input.getPage()).size(input.getSize());
            if (input.getTimeOut() > 0) {
                b.timeout(new TimeValue(input.getTimeOut(), TimeUnit.SECONDS));
            }
            if (input.isSrc_flag()) {
                if (!MiscellanyUtil.isArrayEmpty(input.getSrc_inc()) ||
                        !MiscellanyUtil.isArrayEmpty(input.getSrc_exc())) {
                    String[] includes = input.getSrc_inc() == null ? Strings.EMPTY_ARRAY : input.getSrc_inc();
                    String[] excludes = input.getSrc_exc() == null ? Strings.EMPTY_ARRAY : input.getSrc_exc();
                    b.fetchSource(includes, excludes);
                }
            } else {
                b.fetchSource(false);
            }
            if ("coordinate".equals(input.getSortField())) {
                b.sort(new GeoDistanceSortBuilder("coordinate", input.getLat(), input.getLon()).order(input.getSortOrder()));
            } else if ("_score".equals(input.getSortField())) {
                b.sort(new ScoreSortBuilder().order(SortOrder.DESC));
            } else {
                b.sort(new FieldSortBuilder(input.getSortField()).order(input.getSortOrder()));
            }
            b.query(query(input));
            r.source(b);
            request.add(r);
        }
        return client.msearch(request, RequestOptions.DEFAULT);
    }

    /**
     * multi search by 'terms' query
     * 'terms' query are executed in terms of input.field to matching data sourced from input.ids
     * if input.ids is actually the document ids, then use mget instead.
     * @param input
     * @return
     * @throws Exception
     */
    public SearchResponse multiSearch(EsCompanyInput input) throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(input.getIds().length*2);
        builder.query(QueryBuilders.termsQuery(input.getField(), input.getId()));
        builder.sort("_doc");
        if (input.getTimeOut() > 0) {
            builder.timeout(new TimeValue(input.getTimeOut(), TimeUnit.SECONDS));
        }
        if (input.isSrc_flag()) {
            if (!MiscellanyUtil.isArrayEmpty(input.getSrc_inc()) ||
                    !MiscellanyUtil.isArrayEmpty(input.getSrc_exc())) {
                String[] includes = input.getSrc_inc() == null ? Strings.EMPTY_ARRAY : input.getSrc_inc();
                String[] excludes = input.getSrc_exc() == null ? Strings.EMPTY_ARRAY : input.getSrc_exc();
                builder.fetchSource(includes, excludes);
            }
        } else {
            builder.fetchSource(false);
        }

        return client.search(request.source(builder), RequestOptions.DEFAULT);
    }
}
