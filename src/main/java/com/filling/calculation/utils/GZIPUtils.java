package com.filling.calculation.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @Auther: liangbl
 * @Date: 2018/12/21 13:06
 * @Description:
 */
public class GZIPUtils {
    /**
     * 使用gzip进行压缩
     */
    public static String compress(String primStr) {
        if (primStr == null || primStr.length() == 0) {
            return primStr;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(primStr.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return new sun.misc.BASE64Encoder().encode(out.toByteArray());
    }

    /**
     * 使用gzip进行解压缩
     */
    public static String uncompress(String compressedStr) {
        if (compressedStr == null) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = null;
        GZIPInputStream ginzip = null;
        byte[] compressed = null;
        String decompressed = null;
        try {
            compressed = new sun.misc.BASE64Decoder().decodeBuffer(compressedStr);
            in = new ByteArrayInputStream(compressed);
            ginzip = new GZIPInputStream(in);

            byte[] buffer = new byte[1024];
            int offset = -1;
            while ((offset = ginzip.read(buffer)) != -1) {
                out.write(buffer, 0, offset);
            }
            decompressed = out.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ginzip != null) {
                try {
                    ginzip.close();
                } catch (IOException e) {
                }
            }
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
            try {
                out.close();
            } catch (IOException e) {
            }
        }
        return decompressed;
    }

    public static void main(String[] args) {
        String str =
                "eyJ0cmFuc2Zvcm0iOlt7InNvdXJjZV9maWVsZCI6WyJ2YWx1ZSJdLCJwYXJhbGxlbGlzbSI6IjEiLCJuYW1lIjoiRmllbGRUeXBlQ29udmVyLXRyYW5zZm9ybSIsInNvdXJjZV90YWJsZV9uYW1lIjoia2Fma2FfNjg1YjhmMWNfOWQ1NyIsInJlc3VsdF90YWJsZV9uYW1lIjoiZmllbGRUeXBlQ29udmVyXzgxMzIzYWJiX2VjYTIiLCJwbHVnaW5fbmFtZSI6IkZpZWxkVHlwZUNvbnZlciIsInRhcmdldF9maWVsZF90eXBlIjoiaW50In0seyJzZWxlY3QucmVzdWx0X3RhYmxlX25hbWUiOlsiZGF0YVNlbGVjdG9yXzM3YzY2MjE0X2NkM2ZfdDEiLCJkYXRhU2VsZWN0b3JfMzdjNjYyMTRfY2QzZl90MiJdLCJzZWxlY3Que2lkfV90Mi53aGVyZSI6IiIsInNlbGVjdC57aWR9X3QxLndoZXJlIjoiIiwic2VsZWN0LmRhdGFTZWxlY3Rvcl8zN2M2NjIxNF9jZDNmX3QxLndoZXJlIjoidmFsdWU9NTAiLCJwYXJhbGxlbGlzbSI6IjEiLCJuYW1lIjoiRGF0YVNlbGVjdG9yLXRyYW5zZm9ybSIsInNvdXJjZV90YWJsZV9uYW1lIjoiZmllbGRUeXBlQ29udmVyXzgxMzIzYWJiX2VjYTIiLCJyZXN1bHRfdGFibGVfbmFtZSI6ImRhdGFTZWxlY3Rvcl8zN2M2NjIxNF9jZDNmIiwicGx1Z2luX25hbWUiOiJEYXRhU2VsZWN0b3IiLCJzZWxlY3QuZGF0YVNlbGVjdG9yXzM3YzY2MjE0X2NkM2ZfdDIud2hlcmUiOiJ2YWx1ZTw1MCJ9XSwic2luayI6W3siZXMuYnVsay5mbHVzaC5tYXguYWN0aW9ucyI6MTAwMCwiaG9zdHMiOlsiMTAuMTAuMTQuNTE6OTIwMCJdLCJwYXJhbGxlbGlzbSI6IjEiLCJuYW1lIjoiRWxhc3RpY3NlYXJjaC1zaW5rIiwiaW5kZXgiOiJmaWxsaW5nLTUxIiwiZXMuYnVsay5mbHVzaC5pbnRlcnZhbC5tcyI6MTAwMCwiZXMuYnVsay5mbHVzaC5iYWNrb2ZmLmVuYWJsZSI6InRydWUiLCJzb3VyY2VfdGFibGVfbmFtZSI6ImRhdGFTZWxlY3Rvcl8zN2M2NjIxNF9jZDNmX3QxIiwicGx1Z2luX25hbWUiOiJFbGFzdGljc2VhcmNoIiwiZXMuYnVsay5mbHVzaC5iYWNrb2ZmLmRlbGF5Ijo1MCwiZXMuYnVsay5mbHVzaC5iYWNrb2ZmLnJldHJpZXMiOiI4IiwiZXMuYnVsay5mbHVzaC5tYXguc2l6ZS5tYiI6Mn0seyJlcy5idWxrLmZsdXNoLm1heC5hY3Rpb25zIjoxMDAwLCJob3N0cyI6WyIxMC4xMC4xNC41MTo5MjAwIl0sInBhcmFsbGVsaXNtIjoiMSIsIm5hbWUiOiJFbGFzdGljc2VhcmNoLXNpbmsiLCJpbmRleCI6ImZpbGxpbmctNDkiLCJlcy5idWxrLmZsdXNoLmludGVydmFsLm1zIjoxMDAwLCJlcy5idWxrLmZsdXNoLmJhY2tvZmYuZW5hYmxlIjoidHJ1ZSIsInNvdXJjZV90YWJsZV9uYW1lIjoiZGF0YVNlbGVjdG9yXzM3YzY2MjE0X2NkM2ZfdDIiLCJwbHVnaW5fbmFtZSI6IkVsYXN0aWNzZWFyY2giLCJlcy5idWxrLmZsdXNoLmJhY2tvZmYuZGVsYXkiOjUwLCJlcy5idWxrLmZsdXNoLmJhY2tvZmYucmV0cmllcyI6IjgiLCJlcy5idWxrLmZsdXNoLm1heC5zaXplLm1iIjoyfV0sInNvdXJjZSI6W3sic2NoZW1hIjoie1wiaG9zdFwiOlwiMTkyLjE2OC4xLjEwM1wiLFwic291cmNlXCI6XCJkYXRhc291cmNlXCIsXCJNZXRyaWNzTmFtZVwiOlwiY3B1XCIsXCJ2YWx1ZVwiOlwiNTlcIixcIl90aW1lXCI6MTYyNjU4MTAyMDAwMH0iLCJvZmZzZXQucmVzZXQiOiJlYXJsaWVzdCIsImNvbnN1bWVyLmdyb3VwLmlkIjoibmdpbngiLCJ0b3BpY3MiOiJuZ2lueCIsInBhcmFsbGVsaXNtIjoiMSIsIm5hbWUiOiJrYWZrYS1zb3VyY2UiLCJyZXN1bHRfdGFibGVfbmFtZSI6ImthZmthXzY4NWI4ZjFjXzlkNTciLCJwbHVnaW5fbmFtZSI6IkthZmthVGFibGVTdHJlYW0iLCJmb3JtYXQudHlwZSI6Impzb24iLCJjb25zdW1lci5ib290c3RyYXAuc2VydmVycyI6IjEwLjEwLjE0LjQzOjkwOTIifV0sImVudiI6e319";
        System.out.println("原字符串：" + str);
        System.out.println("原长度：" + str.length());
        String compress = GZIPUtils.compress(str);
        System.out.println("压缩后字符串：" + compress);
        System.out.println("压缩后字符串长度：" + compress.length());
        String string = GZIPUtils.uncompress(compress);
        System.out.println("解压缩后字符串：" + string);
        System.out.println("解压缩后字符串：" + str);
    }
}