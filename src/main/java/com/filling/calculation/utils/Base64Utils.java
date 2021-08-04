package com.filling.calculation.utils;

import java.io.*;
public class Base64Utils {
        public Base64Utils() {
        }

        /**
         * 功能：编码字符串
         *
         * @author jiangshuai
         * @date 2016年10月03日
         * @param data
         *            源字符串
         * @return String
         */
        public static String encode(String data) {
            return new String(encode(data.getBytes()));
        }

        /**
         * 功能：解码字符串
         *
         * @author jiangshuai
         * @date 2016年10月03日
         * @param data
         *            源字符串
         * @return String
         */
        public static String decode(String data) {
            return new String(decode(data.toCharArray()));
        }



        /**
         * 功能：编码byte[]
         *
         * @author jiangshuai
         * @date 2016年10月03日
         * @param data
         *            源
         * @return char[]
         */
        public static char[] encode(byte[] data) {
            char[] out = new char[((data.length + 2) / 3) * 4];
            for (int i = 0, index = 0; i < data.length; i += 3, index += 4) {
                boolean quad = false;
                boolean trip = false;

                int val = (0xFF & (int) data[i]);
                val <<= 8;
                if ((i + 1) < data.length) {
                    val |= (0xFF & (int) data[i + 1]);
                    trip = true;
                }
                val <<= 8;
                if ((i + 2) < data.length) {
                    val |= (0xFF & (int) data[i + 2]);
                    quad = true;
                }
                out[index + 3] = alphabet[(quad ? (val & 0x3F) : 64)];
                val >>= 6;
                out[index + 2] = alphabet[(trip ? (val & 0x3F) : 64)];
                val >>= 6;
                out[index + 1] = alphabet[val & 0x3F];
                val >>= 6;
                out[index + 0] = alphabet[val & 0x3F];
            }
            return out;
        }

        /**
         * 功能：解码
         *
         * @author jiangshuai
         * @date 2016年10月03日
         * @param data
         *            编码后的字符数组
         * @return byte[]
         */
        public static byte[] decode(char[] data) {

            int tempLen = data.length;
            for (int ix = 0; ix < data.length; ix++) {
                if ((data[ix] > 255) || codes[data[ix]] < 0) {
                    --tempLen; // ignore non-valid chars and padding
                }
            }
            // calculate required length:
            // -- 3 bytes for every 4 valid base64 chars
            // -- plus 2 bytes if there are 3 extra base64 chars,
            // or plus 1 byte if there are 2 extra.

            int len = (tempLen / 4) * 3;
            if ((tempLen % 4) == 3) {
                len += 2;
            }
            if ((tempLen % 4) == 2) {
                len += 1;

            }
            byte[] out = new byte[len];

            int shift = 0; // # of excess bits stored in accum
            int accum = 0; // excess bits
            int index = 0;

            // we now go through the entire array (NOT using the 'tempLen' value)
            for (int ix = 0; ix < data.length; ix++) {
                int value = (data[ix] > 255) ? -1 : codes[data[ix]];

                if (value >= 0) { // skip over non-code
                    accum <<= 6; // bits shift up by 6 each time thru
                    shift += 6; // loop, with new bits being put in
                    accum |= value; // at the bottom.
                    if (shift >= 8) { // whenever there are 8 or more shifted in,
                        shift -= 8; // write them out (from the top, leaving any
                        out[index++] = // excess at the bottom for next iteration.
                                (byte) ((accum >> shift) & 0xff);
                    }
                }
            }

            // if there is STILL something wrong we just have to throw up now!
            if (index != out.length) {
                throw new Error("Miscalculated data length (wrote " + index
                        + " instead of " + out.length + ")");
            }

            return out;
        }

        /**
         * 功能：编码文件
         *
         * @author jiangshuai
         * @date 2016年10月03日
         * @param file
         *            源文件
         */
        public static void encode(File file) throws IOException {
            if (!file.exists()) {
                System.exit(0);
            }

            else {
                byte[] decoded = readBytes(file);
                char[] encoded = encode(decoded);
                writeChars(file, encoded);
            }
            file = null;
        }

        /**
         * 功能：解码文件。
         *
         * @author jiangshuai
         * @date 2016年10月03日
         * @param file
         *            源文件
         * @throws IOException
         */
        public static void decode(File file) throws IOException {
            if (!file.exists()) {
                System.exit(0);
            } else {
                char[] encoded = readChars(file);
                byte[] decoded = decode(encoded);
                writeBytes(file, decoded);
            }
            file = null;
        }

        //
        // code characters for values 0..63
        //
        private static char[] alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
                .toCharArray();

        //
        // lookup table for converting base64 characters to value in range 0..63
        //
        private static byte[] codes = new byte[256];
        static {
            for (int i = 0; i < 256; i++) {
                codes[i] = -1;
                // LoggerUtil.debug(i + "&" + codes[i] + " ");
            }
            for (int i = 'A'; i <= 'Z'; i++) {
                codes[i] = (byte) (i - 'A');
                // LoggerUtil.debug(i + "&" + codes[i] + " ");
            }

            for (int i = 'a'; i <= 'z'; i++) {
                codes[i] = (byte) (26 + i - 'a');
                // LoggerUtil.debug(i + "&" + codes[i] + " ");
            }
            for (int i = '0'; i <= '9'; i++) {
                codes[i] = (byte) (52 + i - '0');
                // LoggerUtil.debug(i + "&" + codes[i] + " ");
            }
            codes['+'] = 62;
            codes['/'] = 63;
        }

        private static byte[] readBytes(File file) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] b = null;
            InputStream fis = null;
            InputStream is = null;
            try {
                fis = new FileInputStream(file);
                is = new BufferedInputStream(fis);
                int count = 0;
                byte[] buf = new byte[16384];
                while ((count = is.read(buf)) != -1) {
                    if (count > 0) {
                        baos.write(buf, 0, count);
                    }
                }
                b = baos.toByteArray();

            } finally {
                try {
                    if (fis != null)
                        fis.close();
                    if (is != null)
                        is.close();
                    if (baos != null)
                        baos.close();
                } catch (Exception e) {
                    System.out.println(e);
                }
            }

            return b;
        }

        private static char[] readChars(File file) throws IOException {
            CharArrayWriter caw = new CharArrayWriter();
            Reader fr = null;
            Reader in = null;
            try {
                fr = new FileReader(file);
                in = new BufferedReader(fr);
                int count = 0;
                char[] buf = new char[16384];
                while ((count = in.read(buf)) != -1) {
                    if (count > 0) {
                        caw.write(buf, 0, count);
                    }
                }

            } finally {
                try {
                    if (caw != null)
                        caw.close();
                    if (in != null)
                        in.close();
                    if (fr != null)
                        fr.close();
                } catch (Exception e) {
                    System.out.println(e);
                }
            }

            return caw.toCharArray();
        }

        private static void writeBytes(File file, byte[] data) throws IOException {
            OutputStream fos = null;
            OutputStream os = null;
            try {
                fos = new FileOutputStream(file);
                os = new BufferedOutputStream(fos);
                os.write(data);

            } finally {
                try {
                    if (os != null)
                        os.close();
                    if (fos != null)
                        fos.close();
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }

        private static void writeChars(File file, char[] data) throws IOException {
            Writer fos = null;
            Writer os = null;
            try {
                fos = new FileWriter(file);
                os = new BufferedWriter(fos);
                os.write(data);

            } finally {
                try {
                    if (os != null)
                        os.close();
                    if (fos != null)
                        fos.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }



        public static void main(String[] args) {
            String str = "{\n" +
                    "    \"env\": {\n" +
                    "        \"execution.parallelism\": 1\n" +
                    "    },\n" +
                    "    \"source\": [\n" +
                    "        {\n" +
                    "            \"schema\": \"{\\\"hostid\\\": \\\"host01\\\",\\\"metric\\\": \\\"cpu_user\\\",\\\"value\\\": 13, \\\"auth\\\": \\\"1,2,3,4,5\\\"}\",\n" +
                    "            \"path\": \"/tmp/user.json\",\n" +
                    "            \"result_table_name\": \"FileSourceTable\",\n" +
                    "            \"plugin_name\": \"FileSource\",\n" +
                    "            \"format.type\": \"json\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"schema\": \"{\\\"categoryName\\\":\\\"手机银行\\\",  \\\"department\\\": \\\"开发部\\\", \\\"host\\\": \\\"host01\\\",  \\\"createTime\\\": \\\"2021-01-01: 00:00:00\\\",  \\\"principal\\\": \\\"张三\\\"}\",\n" +
                    "            \"path\": \"/tmp/group.json\",\n" +
                    "            \"result_table_name\": \"FileSource_category\",\n" +
                    "            \"plugin_name\": \"FileSource\",\n" +
                    "            \"format.type\": \"json\"\n" +
                    "        }\n" +
                    "    ],\n" +
                    "    \"transform\": [\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"FileSourceTable\",\n" +
                    "            \"result_table_name\": \"sql_table20\",\n" +
                    "            \"plugin_name\": \"Sql\",\n" +
                    "            \"sql\": \"select * from FileSourceTable where `value` > 13\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"FileSourceTable\",\n" +
                    "            \"result_table_name\": \"sql_table21\",\n" +
                    "            \"plugin_name\": \"DataJoin\",\n" +
                    "            \"join.result_table_name\": [\n" +
                    "                \"FileSource_category\"\n" +
                    "            ],\n" +
                    "            \"join.FileSource_category.where\": \"hostid = host\",\n" +
                    "            \"join.FileSource_category.type\": \"left\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table21\",\n" +
                    "            \"result_table_name\": \"sql_table22\",\n" +
                    "            \"plugin_name\": \"EncodeBase64\",\n" +
                    "            \"source_field\": \"metric\",\n" +
                    "            \"target_field\": \"metric2\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table22\",\n" +
                    "            \"result_table_name\": \"sql_table23\",\n" +
                    "            \"plugin_name\": \"DecodeBase64\",\n" +
                    "            \"source_field\": \"metric2\",\n" +
                    "            \"target_field\": \"metric\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table23\",\n" +
                    "            \"result_table_name\": \"sql_table24\",\n" +
                    "            \"plugin_name\": \"FieldRemove\",\n" +
                    "            \"field\": [\n" +
                    "                \"hostid\"\n" +
                    "            ]\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table24\",\n" +
                    "            \"result_table_name\": \"sql_table25\",\n" +
                    "            \"plugin_name\": \"FieldOrder\",\n" +
                    "            \"field_and_sort\": [\n" +
                    "                \"value.desc\",\n" +
                    "                \"metric.desc\"\n" +
                    "            ]\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table25\",\n" +
                    "            \"result_table_name\": \"sql_table26\",\n" +
                    "            \"plugin_name\": \"FieldRename\",\n" +
                    "            \"source_field\": \"value\",\n" +
                    "            \"target_field\": \"value2\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table26\",\n" +
                    "            \"result_table_name\": \"sql_table27\",\n" +
                    "            \"plugin_name\": \"FieldSelect\",\n" +
                    "            \"field\": [\n" +
                    "                \"categoryName\",\n" +
                    "                \"metric\",\n" +
                    "                \"auth\"\n" +
                    "            ]\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table27\",\n" +
                    "            \"result_table_name\": \"sql_table28\",\n" +
                    "            \"plugin_name\": \"FieldSplit\",\n" +
                    "            \"source_field\": \"auth\",\n" +
                    "            \"fields\": [\n" +
                    "                \"n1\",\n" +
                    "                \"n2\",\n" +
                    "                \"n3\",\n" +
                    "                \"n4\",\n" +
                    "                \"n5\",\n" +
                    "                \"n6\"\n" +
                    "            ],\n" +
                    "            \"separator\": \"\\\\|\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table28\",\n" +
                    "            \"result_table_name\": \"sql_table29\",\n" +
                    "            \"plugin_name\": \"FieldOperation\",\n" +
                    "            \"target_field\": \"newfield\",\n" +
                    "            \"script\": \"concat(n1,n2)\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table29\",\n" +
                    "            \"result_table_name\": \"sql_table30\",\n" +
                    "            \"plugin_name\": \"FieldTypeConver\",\n" +
                    "            \"target_field_type\": \"int\",\n" +
                    "            \"source_field\": [\n" +
                    "                \"newfield\"\n" +
                    "            ]\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table30\",\n" +
                    "            \"result_table_name\": \"sql_table31\",\n" +
                    "            \"plugin_name\": \"FieldSelect\",\n" +
                    "            \"field\": [\n" +
                    "                \"n1\",\n" +
                    "                \"n2\",\n" +
                    "                \"newfield0\"\n" +
                    "            ]\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"source_table_name\": \"sql_table31\",\n" +
                    "            \"result_table_name\": \"sql_table32\",\n" +
                    "            \"plugin_name\": \"DataSelector\",\n" +
                    "            \"select.result_table_name\": [\n" +
                    "                \"sql_table33\",\n" +
                    "                \"sql_table34\"\n" +
                    "            ],\n" +
                    "            \"select.sql_table33.where\": \" n1 ='1'\",\n" +
                    "            \"select.sql_table34.where\": \" n1 !='1'\"\n" +
                    "        }\n" +
                    "    ],\n" +
                    "    \"sink\": [\n" +
                    "        {\n" +
                    "            \"path\": \"/tmp/1\",\n" +
                    "            \"write_mode\": \"OVERWRITE\",\n" +
                    "            \"format\": \"json\",\n" +
                    "            \"source_table_name\": \"sql_table32\",\n" +
                    "            \"plugin_name\": \"FileSink\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"path\": \"/tmp/4\",\n" +
                    "            \"write_mode\": \"OVERWRITE\",\n" +
                    "            \"format\": \"json\",\n" +
                    "            \"source_table_name\": \"sql_table31\",\n" +
                    "            \"plugin_name\": \"FileSink\"\n" +
                    "        }\n" +
                    "    ]\n" +
                    "}";

            String str2 = "{\n" +
                    "  \"env\": {\n" +
                    "    \"execution.parallelism\": 1,\n" +
                    "    \"execution.time-characteristic\": \"event-time\",\n" +
                    "    \"job.name\": \"wattttt\"\n" +
                    "  },\n" +
                    "  \"source\": [\n" +
                    "    {\n" +
                    "      \"plugin_name\": \"DataGenTableStream\",\n" +
                    "      \"result_table_name\": \"dataGenTableStreamTable\",\n" +
                    "      \"schema\": \"{\\\"id\\\":1, \\\"host\\\":\\\"192.168.1.103\\\",\\\"source\\\":\\\"datasource\\\",\\\"MetricsName\\\":\\\"cpu\\\",\\\"value\\\":49}\",\n" +
                    "      \"rows-per-second\": 10,\n" +
                    "      \"number-of-rows\": 100000000,\n" +
                    "      \"fields\": [\n" +
                    "        {\n" +
                    "          \"id\": {\n" +
                    "            \"type\": \"Int\",\n" +
                    "            \"min\": 1,\n" +
                    "            \"max\": 2\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"host\": {\n" +
                    "            \"type\": \"String\",\n" +
                    "            \"length\": 5\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"source\": {\n" +
                    "            \"type\": \"String\",\n" +
                    "            \"length\": 10\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"MetricsName\": {\n" +
                    "            \"type\": \"String\",\n" +
                    "            \"length\": 10\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"value\": {\n" +
                    "            \"type\": \"Int\",\n" +
                    "            \"min\": 1,\n" +
                    "            \"max\": 2\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"parallelism\": 2,\n" +
                    "      \"name\": \"my-datagen-source\"\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  \"transform\": [\n" +
                    "    {\n" +
                    "      \"source_table_name\": \"dataGenTableStreamTable\",\n" +
                    "      \"result_table_name\": \"FieldOperation_time\",\n" +
                    "      \"plugin_name\": \"FieldOperation\",\n" +
                    "      \"target_field\": \"_time\",\n" +
                    "      \"script\": \"UNIX_TIMESTAMP()*1000\"\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  \"sink\": [\n" +
                    "    {\n" +
                    "      \"source_table_name\": \"FieldOperation_time\",\n" +
                    "      \"plugin_name\": \"KafkaTable\",\n" +
                    "      \"producer.bootstrap.servers\": \"192.168.1.218:9092\",\n" +
                    "      \"topics\": \"test\",\n" +
                    "      \"parallelism\": 2,\n" +
                    "      \"name\": \"my-kafka-sink\"\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}";
            System.out.println(encode(str2));
        }

    }
