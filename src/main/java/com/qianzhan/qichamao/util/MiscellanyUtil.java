package com.qianzhan.qichamao.util;

import org.apache.http.util.Asserts;

import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class MiscellanyUtil {
    public static boolean isBlank(String s) {
        return s == null || s.length() == 0 || s.trim().length() == 0;
    }

    public static boolean isComposedWithAscii(String s) {
        if (isBlank(s)) return true;
        for (int i = 0; i < s.length(); ++i) {
            if (s.charAt(i) > 128) {
                return false;
            }
        }
        return true;
    }



//    public static String trimNonChinese(String text) {
//        if (isBlank(text)) return null;
//
//        StringBuilder sb = new StringBuilder();
//        int m = 0, n = 0;
//        char[] chars = text.toCharArray();
//        for (int i = 0, j = chars.length-1; i < chars.length; ++i,--j) {
//            char c = chars[i];
//            if ((c >= '\u4e00' && c <= '\u9fa5') ||
//        }
//    }

    public static int getEditDistanceSafe(String text1, String text2) throws Exception {
        if (isBlank(text1) && isBlank(text2)) throw new Exception("text1 and text2 can not be empty at same time.");
        if (isBlank(text1)) return text2.length();
        if (isBlank(text2)) return text1.length();
        if (text1.equals(text2)) return 0;
        return levenshteinDistance(text1, text2);
    }

    public static int levenshteinDistanceRecur(String text1, String text2) {
        if (text1.length() == 0) return text2.length();
        if (text2.length() == 2) return text1.length();
        if (text1.equals(text2)) return 0;

        int d = 0;
        if (text1.charAt(text1.length()-1) != text2.charAt(text2.length()-1))
            d = 1;

        String prefix1 = new String(text1.toCharArray(), 0, text1.length()-1);
        String prefix2 = new String(text2.toCharArray(), 0, text2.length()-1);
        return min(levenshteinDistanceRecur(text1, prefix2)+1,
                levenshteinDistanceRecur(prefix1, text2)+1,
                levenshteinDistanceRecur(prefix1, prefix2)+d);
    }

    private static int levenshteinDistance(String text1, String text2) {
        int[][] matrix = new int[text1.length()+1][text2.length()+1];
        for (int i = 0; i < text1.length()+1; ++i) {
            matrix[i][0] = i;
        }
        for (int j = 0; j < text2.length()+1; ++j) {
            matrix[0][j] = j;
        }

        for (int i = 1; i < text1.length()+1; ++i) {
            for (int j = 1; j < text2.length()+1; ++j) {
                int d = text1.charAt(i-1) == text2.charAt(j-1) ? 0 : 1;
                matrix[i][j] = min(matrix[i-1][j]+1, matrix[i][j-1]+1, matrix[i-1][j-1]+d);
            }
        }
        return matrix[text1.length()][text2.length()];
    }

    public static int min(int... ints) {
        Asserts.check(ints.length > 0, "provide one parameter at least.");
        int min = Integer.MAX_VALUE;
        for (int i : ints) {
            if (min > i)
                min = i;
        }
        return min;
    }


    

    public static <T> boolean isArrayEmpty(T[] array) {
        return array == null || array.length == 0;
    }

    public static <T> boolean isArrayEmpty(Collection<T> collection) {
        return collection == null || collection.size() == 0;
    }

    public static <T> void testClass(List<T> list) {
        ParameterizedType paramType = (ParameterizedType)list.getClass().getGenericSuperclass();
        Class<T> clazz = (Class) paramType.getActualTypeArguments()[0];

        Class<T> clazz1 = (Class<T>) list.iterator().next().getClass();
        if (clazz == clazz1)
            System.out.println("the same object");
        else
            System.out.println("not the same object");
    }

    public static byte[] int2bytes(int i) {
        byte[] bytes = new byte[4];
        bytes[3] = (byte) ((i >> 24) & 0xFF);
        bytes[2] = (byte) ((i >> 16) & 0xFF);
        bytes[1] = (byte) ((i >> 8) & 0xFF);
        bytes[0] = (byte) (i & 0xFF);
        return bytes;
    }

    public static int bytes2int(byte[] bytes) {
        return bytes2int(bytes, 0);
    }

    public static int bytes2int(byte[] bytes, int start) {
        return (bytes[start]&0xFF)|((bytes[start+1]&0xFF)<<8)|((bytes[start+2]&0xFF)<<16)|((bytes[start+3] & 0xFF)<<24);
    }
}
