package com.qcm.util;


import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;

import javax.crypto.*;
import javax.crypto.spec.DESKeySpec;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Random;

public class Cryptor {
    public static String encrypt_des(String text) { return encrypt_des(text, null); }
    public static String encrypt_des(String text, String password) {
        if (MiscellanyUtil.isBlank(password)) {
            password = "12345678";
        }
        if (password.length() % 8 != 0) {
            throw new ValueException("password length must be times of 8");
        }

        try {
            SecureRandom random = new SecureRandom();
            DESKeySpec desKey = new DESKeySpec(password.getBytes("utf-8"));
            SecretKeyFactory factory = SecretKeyFactory.getInstance("DES");
            SecretKey key = factory.generateSecret(desKey);
            Cipher cipher =  Cipher.getInstance("DES");
            cipher.init(Cipher.ENCRYPT_MODE, key, random);
            byte[] bytes = cipher.doFinal(text.getBytes("utf-8"));
            Base64.Encoder encoder = Base64.getEncoder();
            return encoder.encodeToString(bytes);
        } catch (InvalidKeyException | NoSuchAlgorithmException | InvalidKeySpecException | NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return text;
    }

    public static String decrypt_des(String text) { return decrypt_des(text, null); }
    public static String decrypt_des(String text, String password) {
        if (MiscellanyUtil.isBlank(password)) {
            password = "12345678";
        }

        try {
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] bytes = decoder.decode(text);
            SecureRandom random = new SecureRandom();
            DESKeySpec desKey = new DESKeySpec(password.getBytes("utf-8"));
            SecretKeyFactory factory = SecretKeyFactory.getInstance("DES");
            SecretKey key = factory.generateSecret(desKey);
            Cipher cipher = Cipher.getInstance("DES");
            cipher.init(Cipher.DECRYPT_MODE, key, random);
            return new String(cipher.doFinal(bytes), "utf-8");
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return text;
    }

    public static String md5(String text) {
        if (MiscellanyUtil.isBlank(text)) return "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(text.getBytes("gbk"));
            byte[] bytes = md.digest();
            String code = new BigInteger(1, bytes).toString(16);
            return code;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * only used to encode text which is composed with '0-9a-z'
     * @param text
     * @return
     */
    public static String shiftEncode(String text) throws Exception {
        if (MiscellanyUtil.isBlank(text)) return null;
        int len = 10 + 26;      // characters must be '0-9a-z'.
        Random r = new Random();
        int radix = r.nextInt(6)+3;

        StringBuilder result = new StringBuilder();
        if (r.nextInt(1000) > 10) {
            result.append('0');
        } else {
            result.append('1');
        }
        int count = 0;
        int[] digits = new int[4];
        int sum = 0;
        while (count < 4) {
            int d = r.nextInt(10);
            if (d != radix) {
                digits[count] = d;
                count++;
                sum+=d;
            }
        }
        int position = sum % 5;
        for (int i = 0; i < 4; i++) {
            if (i == position)
                result.append(radix);
            result.append(digits[i]);
        }
        int index;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c >= '0' && c <= '9') {
                index = c - '0' + radix;
            } else if (c >= 'a' && c <= 'z') {
                index = c - 'a' + radix + 10;
            } else throw new Exception("invalid input: " + text);
            if (index >= len) index -= len;
            if (index < 10) result.append((char) ('0'+index));
            else result.append((char) ('a'+index-10));
        }
        return result.toString();
    }

    public static String shiftDecode(String text) throws Exception {
        if (MiscellanyUtil.isBlank(text)) return null;
        if (text.length() < 6) throw new Exception("cannot decode");
        int i = 1;
        int sum = 0;
        int len = 10 + 26;      // characters must be '0-9a-z'.

        for (;i<6;i++) {
            int d = text.charAt(i) - '0';
            if (d >= 0 && d <= 9) {
                sum += d;
            } else throw new Exception("cannot decode");
        }
        StringBuilder result = new StringBuilder();
        int position = sum % 5;
        int radix = text.charAt(position+1) - '0';
        int index;
        for (; i < text.length(); i++) {
            char c = text.charAt(i);

            if (c >= '0' && c <= '9') {
                index = c - '0';
            } else if (c >= 'a' && c <= 'z') {
                index = c - 'a';
            } else throw new Exception("invalid input: " + text);

            index -= radix;
            if (index < 0) index += len;
            if (index < 10) result.append((char) ('0'+index));
            else result.append((char) ('a'+index-10));
        }
        return result.toString();
    }
}
