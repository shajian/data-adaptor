package com.qianzhan.qichamao.dal.es;

import com.qianzhan.qichamao.util.EsConfigBus;
import lombok.Getter;
import lombok.Setter;
import org.apache.http.util.Asserts;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Getter
@Setter
public class EsAggSetting {
    //======================= simple aggregation ========================
    private Set<String> terms;
    private Map<String, String[]> filters;  //
    private Map<String, int[]> ranges;
    private Map<String, double[]> geos;
    private Map<String, DateHistogramSetting> dateHistograms;
    //======================= simple aggregation ========================

    //======================= complex aggregation =======================
    private List<Map.Entry<String, String>> double_terms;
    private List<Map.Entry<AggCriteria, AggCriteria>> double_universes;
    //======================= complex aggregation =======================


    public void shrink(Map map) {
        for (Object key : map.keySet()) {
            if (terms.contains(key)) {
                terms.remove(key);
            } else if (ranges.containsKey(key)) {
                ranges.remove(key);
            } else if (dateHistograms.containsKey(key)) {
                dateHistograms.remove(key);
            } else if (filters.containsKey(key)) {
                filters.remove(key);
            }
            // geo need not to be shrunk
        }
    }

    private static Map<String, EsAggSetting> cache = new HashMap<>();
    private static ReadWriteLock lock = new ReentrantReadWriteLock();

    public static EsAggSetting getDefault(String path) {
        Asserts.check(path.endsWith("aggs"), "path must ends with 'aggs'");
        lock.readLock().lock();
        EsAggSetting set = getDefault(path);
        lock.readLock().unlock();
        if (set != null) return set;
        lock.writeLock().lock();
        set = getDefault(path);
        if (set == null) {
            set = loadFromConfig(path);
            cache.put(path, set);
        }
        lock.writeLock().unlock();
        return set;
    }

    public static EsAggSetting companyDefault() {
        return getDefault("company.aggs");
    }
    public static EsAggSetting loadFromConfig(String path) {
        Asserts.check(path.endsWith("aggs"), "path must ends with 'aggs'");
        Map<String, Object> map = EsConfigBus.get(path);
        EsAggSetting set = new EsAggSetting();
        for (String key : map.keySet()) {
            String[] values = ((String) map.get(key)).split(" ");
            if ("term".equals(values[0])) {
                if (set.terms == null)
                    set.terms = new HashSet<>();
                set.terms.add(key);
            } else if ("range".equals(values[0])) {
                if (set.ranges == null)
                    set.ranges = new HashMap<>();
                int[] ints = new int[values.length-1];
                for (int i = 1; i < values.length; ++i) {
                    ints[i-1] = Integer.getInteger(values[i]);
                }
                set.ranges.put(key, ints);
            } else if ("date_histogram".equals(values[0])) {
                if (set.dateHistograms == null)
                    set.dateHistograms = new HashMap<>();
                String interval = values[1];
                String min = null, max = null;
                if (values.length > 2) {
                    min = values[2];
                    if (values.length > 3)
                        max = values[3];
                }
                set.dateHistograms.put(key, new DateHistogramSetting(interval, min, max));
            } else if ("filter".equals(values[0])) {
                if (set.filters == null)
                    set.filters = new HashMap<>();
                String[] vals = new String[values.length - 1];
                for (int i = 1; i < values.length; ++i)
                    vals[i-1] = values[i];
                set.filters.put(key, vals);
            }
        }
        return set;
    }

    public enum AggType {
        term,
        filter,
        range,
        geo,
        date_histogram
    }
    @Getter@Setter
    public static class AggCriteria {
        private Map<AggType, Object> map;
        private String name;
    }
    @Getter@Setter
    public static class DateHistogramSetting {
        private DateHistogramInterval interval;
        private String min;
        private String max;

        public DateHistogramSetting(String _interval, String _min, String _max) {
            if ("year".equals(_interval)) {
                interval = DateHistogramInterval.YEAR;
            } else if ("quarter".equals(_interval)) {
                interval = DateHistogramInterval.QUARTER;
            } else if ("month".equals(_interval)) {
                interval = DateHistogramInterval.MONTH;
            } else if ("week".equals(_interval)) {
                interval = DateHistogramInterval.WEEK;
            } else if ("day".equals(_interval)) {
                interval = DateHistogramInterval.DAY;
            } else {
                interval = DateHistogramInterval.YEAR;
            }
            if (_min != null && "bigbang".equals(_min)) {
                min = _min;
            }
            if (_max != null) {
                max = _max;
                if ("now".equals(max)) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");//注意月份是MM
                    max = simpleDateFormat.format(new Date());
                }
            }
        }
    }
}
