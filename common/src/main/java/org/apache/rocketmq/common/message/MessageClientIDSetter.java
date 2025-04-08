/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.message;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.UtilAll;

/**
 *  ipv4 地址转换为数组长度 4
 *  ipv4 地址转换为数组长度 16
 */
public class MessageClientIDSetter {

    /**
     *  LEN = ip.length + 12 =  16/28 ;
     *
     *  2：  pid
     *  4：  MessageClientIDSetter 类加载器的 hashcode
     *  4：
     */
    private static final int LEN;
    /**
     * 每个客户端的消息的 前缀都是固定的
     *
     * ip + pid + this.hashCode()
     * ip.length + 2 + 4
     *
     * s = 4/16 + 2 + 4 = 10/22
     *
     * 长度 20/44
     */
    private static final char[] FIX_STRING;
    private static final AtomicInteger COUNTER;
    private static long startTime;
    private static long nextStartTime;

    static {
        byte[] ip;
        try {
            ip = UtilAll.getIP();
        } catch (Exception e) {
            // System.currentTimeMillis() 的后四位字节
            // 4字节
            ip = createFakeIP();
        }
        // 16
        LEN = ip.length + 2 + 4 + 4 + 2;
        ByteBuffer tempBuffer = ByteBuffer.allocate(ip.length + 2 + 4);
        tempBuffer.put(ip);
        tempBuffer.putShort((short) UtilAll.getPid());
        tempBuffer.putInt(MessageClientIDSetter.class.getClassLoader().hashCode());
        //
        FIX_STRING = UtilAll.bytes2string(tempBuffer.array()).toCharArray();
        setStartTime(System.currentTimeMillis());
        COUNTER = new AtomicInteger(0);
    }

    private synchronized static void setStartTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        // 本月开始时间
        startTime = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, 1);
        // 下月开始时间
        nextStartTime = cal.getTimeInMillis();
    }

    public static Date getNearlyTimeFromID(String msgID) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        byte[] bytes = UtilAll.string2bytes(msgID);
        int ipLength = bytes.length == 28 ? 16 : 4;
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put(bytes, ipLength + 2 + 4, 4);
        buf.position(0);
        long spanMS = buf.getLong();
        Calendar cal = Calendar.getInstance();
        long now = cal.getTimeInMillis();
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        long monStartTime = cal.getTimeInMillis();
        if (monStartTime + spanMS >= now) {
            cal.add(Calendar.MONTH, -1);
            monStartTime = cal.getTimeInMillis();
        }
        cal.setTimeInMillis(monStartTime + spanMS);
        return cal.getTime();
    }

    public static String getIPStrFromID(String msgID) {
        byte[] ipBytes = getIPFromID(msgID);
        if (ipBytes.length == 16) {
            return UtilAll.ipToIPv6Str(ipBytes);
        } else {
            return UtilAll.ipToIPv4Str(ipBytes);
        }
    }

    public static byte[] getIPFromID(String msgID) {
        byte[] bytes = UtilAll.string2bytes(msgID);
        int ipLength = bytes.length == 28 ? 16 : 4;
        byte[] result = new byte[ipLength];
        System.arraycopy(bytes, 0, result, 0, ipLength);
        return result;
    }

    public static int getPidFromID(String msgID) {
        byte[] bytes = UtilAll.string2bytes(msgID);
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        int value = wrap.getShort(bytes.length - 2 - 4 - 4 - 2);
        return value & 0x0000FFFF;
    }

    /**
     * 对ipv4 来说长度 32
     *
     * 20 字符：固定前缀，每个客户端的实例的 消息的前缀是相同的
     * 8 字符：当前时间与月初时间的毫秒差值。通过无符号右移，获取8个16进制的字符
     * 4 字符：从0开始，自增值。通过无符号右移，获取4个16进制的字符
     *
     * 对ipv6 来说长度 56
     *
     * 44 字符：固定前缀，每个客户端的实例的 消息的前缀是相同的
     * 8 字符：当前时间与月初时间的毫秒差值。通过无符号右移，获取8个16进制的字符
     * 4 字符：从0开始，自增值。通过无符号右移，获取4个16进制的字符
     */
    public static String createUniqID() {
        char[] sb = new char[LEN * 2];
        System.arraycopy(FIX_STRING, 0, sb, 0, FIX_STRING.length);

        // 每月更新时间
        long current = System.currentTimeMillis();
        if (current >= nextStartTime) {
            setStartTime(current);
        }
        // 处理 NTP 导致的 时钟回拨问题
        int diff = (int)(current - startTime);
        if (diff < 0 && diff > -1000_000) {
            // may cause by NTP
            diff = 0;
        }
        // 20
        int pos = FIX_STRING.length;
        // 8 个 char
        UtilAll.writeInt(sb, pos, diff);
        pos += 8;
        // 4 个char
        UtilAll.writeShort(sb, pos, COUNTER.getAndIncrement());

        return new String(sb);
    }

    public static void setUniqID(final Message msg) {
        if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null) {
            // 共 32 字符
            msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
        }
    }

    public static String getUniqID(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }

    public static byte[] createFakeIP() {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(System.currentTimeMillis());
        bb.position(4);
        byte[] fakeIP = new byte[4];
        bb.get(fakeIP);
        return fakeIP;
    }
}
