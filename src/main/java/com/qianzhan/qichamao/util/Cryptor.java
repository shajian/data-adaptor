package com.qianzhan.qichamao.util;

import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;

import javax.crypto.*;
import javax.crypto.spec.DESKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

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
}
