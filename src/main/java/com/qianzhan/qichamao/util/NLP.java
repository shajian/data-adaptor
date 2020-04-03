package com.qianzhan.qichamao.util;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;

import java.util.List;

public class NLP {
    private static Segment segment = HanLP.newSegment().enableNameRecognize(true)
            .enableJapaneseNameRecognize(true).enableOrganizationRecognize(true);

    /**
     * 0: unknown
     * 1: company name
     * 2: natural person name
     * @param text
     * @return
     */
    public static int recognizeName(String text) {
        if (MiscellanyUtil.isBlank(text)) return 0;
        List<Term> terms = segment.seg(text);
        boolean is_short = text.length() < 8;
        boolean person = false;
        for (Term t : terms) {
            Nature n = t.nature;
            if (n == Nature.nr || n == Nature.nrj || n == Nature.nr2 || n ==Nature.nr1) {
                if (is_short) return 2;
                if (!person) person = true;
                else return 2;
            }
            if (n == Nature.nt || n == Nature.ntc || n == Nature.ntcf || n == Nature.ntcb
                    || n == Nature.nto || n == Nature.ntu || n == Nature.nts || n == Nature.nth) {
                return 1;
            }
        }
        if (person) return 2;
        if (is_short) return 2;
        return 1;
    }

}
