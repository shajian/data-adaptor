package com.qianzhan.qichamao.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import org.bson.Document;

public class BeanUtil {
    public static Map bean2Map(Object obj) throws Exception {
        Class clazz = obj.getClass();
        Map map = new HashMap();
        BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
        PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
        for(PropertyDescriptor descriptor : descriptors) {
            String name = descriptor.getName();
            Method read = descriptor.getReadMethod();
            Object value = read.invoke(obj);
            if (value != null) {
                map.put(name, value);
            } else {
                map.put(name, "");
            }
        }
        return map;
    }

    public static <T> T map2Bean(Map map, Class<T> clazz) {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
            T obj = clazz.newInstance();
            PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor descriptor : descriptors) {
                String name = descriptor.getName();
                if (name.equals("class")) continue;
                if (map.containsKey(name)) {
                    Class type = descriptor.getPropertyType();
                    Object val = map.get(name);
                    if (type.isInstance(val))
                        descriptor.getWriteMethod().invoke(obj, type.cast(val));
                    else if (type.isPrimitive()) {
                        String typeName = type.getName();
                        Method method = val.getClass().getMethod(typeName + "Value");
                        descriptor.getWriteMethod().invoke(obj, method.invoke(val));
                    }
//                if (type.isArray()) {
//                    Class subType = type.getComponentType();
//                }
                }
            }
            return obj;
        } catch (Exception e) {
            // log
        }
        return null;
    }

    public static Document bean2Doc(Object obj) throws Exception {
        return new Document(bean2Map(obj));
    }

    public static <T> T doc2Bean(Document doc, Class<T> clazz) {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
            T obj = clazz.newInstance();
            PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor descriptor : descriptors) {
                String name = descriptor.getName();
                if (name.equals("class")) continue;
                if (doc.containsKey(name)) {
                    Class type = descriptor.getPropertyType();
                    Object val = doc.get(name);
                    if (type.isInstance(val))
                        descriptor.getWriteMethod().invoke(obj, type.cast(val));
                    else if (type.isPrimitive()) {
                        String typeName = type.getName();
                        Method method = val.getClass().getMethod(typeName + "Value");
                        descriptor.getWriteMethod().invoke(obj, method.invoke(val));
                    }
//                if (type.isArray()) {
//                    Class subType = type.getComponentType();
//                }
                }
            }
            return obj;
        } catch (Exception e) {
            // log
        }
        return null;
    }

    public static <T> T doc2Obj(Document doc, Class<T> clazz) {
        return JSON.parseObject(doc.toJson(), clazz);
    }

    public static Document obj2Doc(Object obj) {
        return Document.parse(JSON.toJSONString(obj));
    }
}
