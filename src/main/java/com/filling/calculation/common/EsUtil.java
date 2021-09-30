package com.filling.calculation.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zihjiang
 * @date 2020/10/22 - 15:10
 */
public class EsUtil {

//    public static Map<String, Object> rowToJsonMap(Row row, List<String> fields, List<TypeInformation> types) {
//        Preconditions.checkArgument(row.getArity() == fields.size());
//        Map<String,Object> jsonMap = Maps.newHashMap();
//        int i = 0;
//        for(; i < fields.size(); ++i) {
//            String field = fields.get(i);
//            String[] parts = field.split("\\.");
//            Map<String, Object> currMap = jsonMap;
//            for(int j = 0; j < parts.length - 1; ++j) {
//                String key = parts[j];
//                if(currMap.get(key) == null) {
//                    HashMap<String, Object> hashMap = Maps.newHashMap();
//                    currMap.put(key, hashMap);
//                }
//                currMap = (Map<String, Object>) currMap.get(key);
//            }
//            String key = parts[parts.length - 1];
//            Object col = row.getField(i);
//            if(col != null) {
//                Object value = DtStringUtil.col2string(col, types.get(i));
//                currMap.put(key, value);
//            }
//
//        }
//
//        return jsonMap;
//    }

    public static Map<String, Object> rowToJsonMap(Row row, List<String> fields, List<TypeInformation> types) {
        Preconditions.checkArgument(row.getArity() == fields.size());
        Map<String, Object> jsonMap = Maps.newHashMap();
        int i = 0;
        for (; i < fields.size(); ++i) {
            String field = fields.get(i);
            String[] parts = field.split("\\.");
            Map<String, Object> currMap = jsonMap;
            for (int j = 0; j < parts.length - 1; ++j) {
                String key = parts[j];
                if (currMap.get(key) == null) {
                    HashMap<String, Object> hashMap = Maps.newHashMap();
                    currMap.put(key, hashMap);
                }
                currMap = (Map<String, Object>) currMap.get(key);
            }
            String key = parts[parts.length - 1];
            Object col = row.getField(i);
            if (col != null) {
                System.out.println("types.get(i).getClass().getTypeName(): " + types.get(i).getClass().getTypeName());
                if (types.get(i).isBasicType()) {
                    Object value = DtStringUtil.col2string(col, types.get(i).toString());
                    currMap.put(key, value);
                } else {
                    switch (types.get(i).getClass().getTypeName()) {
                        case "org.apache.flink.api.java.typeutils.RowTypeInfo":
                            currMap.put(key, rowObjectToJsonMap(col));
                            break;
                        case "org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo":
                            currMap.put(key, rowArrayToJsonMap(col));
                            break;
                    }


                }
            }

        }

        return jsonMap;
    }

    static Map<String, Object> rowObjectToJsonMap(Object col) {
        Row row = (Row) col;
        Map result = new HashMap(row.getArity());
        for (String fieldName : row.getFieldNames(true)) {
            if( "org.apache.flink.api.java.typeutils.RowTypeInfo".equals(row.getFieldAs(fieldName).getClass().getTypeName()) ) {
                result.put(fieldName, rowObjectToJsonMap(row.getFieldAs(fieldName)));
            } else if("org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo".equals(row.getFieldAs(fieldName).getClass().getTypeName())) {
                result.put(fieldName, rowArrayToJsonMap(row.getFieldAs(fieldName)));
            } else {
                result.put(fieldName, row.getFieldAs(fieldName).toString());
            }
        }
        return result;

    }

    static List rowArrayToJsonMap(Object col) {

        List result = new ArrayList();
        System.out.println("col.getClass(): " + col.getClass().getTypeName());
        Row[] rows = (Row[]) col;
        for (Row row : rows) {
            for (String fieldName : row.getFieldNames(true)) {
                if( "org.apache.flink.api.java.typeutils.RowTypeInfo".equals(row.getFieldAs(fieldName).getClass().getTypeName()) ) {
                    Map<String, Object> _result = new HashMap<>();
                    _result.put(fieldName, rowObjectToJsonMap(row.getFieldAs(fieldName)));
                    result.add(_result);
                } else if("org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo".equals(row.getFieldAs(fieldName).getClass().getTypeName())) {
                    List<Map<String, Object>> _result = rowArrayToJsonMap(row.getFieldAs(fieldName));
                    result.addAll(_result);
                } else {
                    result.add(row.getField(fieldName));
                }
            }
        }

        return result;
    }


}
