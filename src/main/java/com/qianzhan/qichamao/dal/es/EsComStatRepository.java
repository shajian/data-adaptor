package com.qianzhan.qichamao.dal.es;

import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EsComStatRepository extends EsBaseRepository<EsComStat> {
    public EsComStatRepository() {
        super();
    }

    public SearchResponse search(EsComStatInput input) throws IOException {
        if (input.getAccessMode() == 2 && !MiscellanyUtil.isBlank(input.getScrollId())) {   // next scroll step
            SearchScrollRequest request = new SearchScrollRequest(input.getScrollId());
            request.scroll(TimeValue.timeValueSeconds(30));
            return client.scroll(request, RequestOptions.DEFAULT);
        }
        SearchRequest request = new SearchRequest(indexMeta.index());
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(input.getSize());
        if (input.getAccessMode() == 0) builder.from(input.getFrom()*input.getSize());
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
            builder.sort(new GeoDistanceSortBuilder("coordinate", input.getLat(), input.getLon())
                    .order(input.getSortOrder()));
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
        if (input.getAggs() != null && input.getAccessMode() != 2 && input.getFrom() == 0) {
            for (AggregationBuilder agg : aggregate(input)) {
                builder.aggregation(agg);
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

    private QueryBuilder query(EsComStatInput input) {
        return QueryBuilders.boolQuery().filter(filter(input)).must(must(input));
    }

    private QueryBuilder filter(EsComStatInput input) {
        // filter: single-value-mode
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        for (String field : input.getFilters().keySet()) {
            String value = input.getFilters().get(field);
            if ("coordinate".equals(field)) {
                String[] segs = value.split(",");
                bool.must(QueryBuilders.geoDistanceQuery(field).point(input.getLat(), input.getLon())
                        .distance(segs[2]+"km"));
            } else if ("establish_date".equals(field)) {
                int year = Integer.parseInt(value);
                int next_year = year+1;
                bool.must(QueryBuilders.rangeQuery(field).gte(year+"-01-01").lt(next_year+"-01-01"));
            } else if ("register_money".equals(field)) {
                String[] minmax = value.split("-");
                bool.must(QueryBuilders.rangeQuery(field).gte(minmax[0]).lt(minmax[1]));
            } else {
                bool.must(QueryBuilders.termQuery(field, value));
            }
        }
        return bool;
    }

    private QueryBuilder must(EsComStatInput input) {
        // only search for oc_name
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        bool.should(QueryBuilders.termQuery("oc_name.key", input.getKeyword()))
                .should(QueryBuilders.matchPhraseQuery("oc_name", input.getKeyword()))
                .should(QueryBuilders.matchPhraseQuery("oc_name.crf", input.getKeyword()))
                .should(QueryBuilders.matchPhraseQuery("oc_name.nlp", input.getKeyword()));
        return bool;
    }

    private List<AggregationBuilder> aggregate(EsComStatInput input) {
        EsAggSetting aggs = input.getAggs();
        aggs.shrink(input.getFilters());
        List<AggregationBuilder> list = new ArrayList<>();
        // simple: one-env aggregations
        if (aggs.getTerms() != null) {
            for (String field : aggs.getTerms()) {
                TermsAggregationBuilder term = AggregationBuilders.terms(field).field(field);
                list.add(term);
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
                list.add(range);
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
                list.add(geo);
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
                    list.add(filter);
                }
                else {
                    DateHistogramAggregationBuilder date = AggregationBuilders.dateHistogram(field)
                            .dateHistogramInterval(dh.getInterval());
                    list.add(date);
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
                list.add(filters);
            }
        }

        // complex: two-env aggregations
        if (aggs.getDouble_terms() != null) {

        }
        return list;
    }
}
