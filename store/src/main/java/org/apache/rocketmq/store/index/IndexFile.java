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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    /**
     * Each index's store unit. Format:
     * <pre>
     * ┌───────────────┬───────────────────────────────┬───────────────┬───────────────┐
     * │ Key HashCode  │        Physical Offset        │   Time Diff   │ Next Index Pos│
     * │   (4 Bytes)   │          (8 Bytes)            │   (4 Bytes)   │   (4 Bytes)   │
     * ├───────────────┴───────────────────────────────┴───────────────┴───────────────┤
     * │                                 Index Store Unit                              │
     * │                                                                               │
     * </pre>
     * Each index's store unit. Size:
     * Key HashCode(4) + Physical Offset(8) + Time Diff(4) + Next Index Pos(4) = 20 Bytes
     */
    private static int indexSize = 20;
    private static int invalidIndex = 0;

    /**
     * 500 0000
     * 这个值 会 增加
     */
    private final int hashSlotNum;

    /**
     *
     * 文件中最大保存的索引数量，默认 500 0000 * 4
     * 和文件头中的索引数量比较 用来判断文件是够写满
     */
    private final int indexNum;
    private final int fileTotalSize;
    private final MappedFile mappedFile;
    private final MappedByteBuffer mappedByteBuffer;
    /**
     *  Begin Timestamp(8) + End Timestamp(8) + Begin Physical Offset(8)
     *  + End Physical Offset(8) + Hash Slot Count(4) + Index Count(4) = 40 Bytes
     *
     *  Begin/End Timestamp：在 commitLog 中的存储时间
     *  Begin/End Physical Offset：在 commitLog 中的绝对偏移量
     */
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {

        this.fileTotalSize =
                // 40            +       （ 500 0000 *4）                  + （500 0000 * 4 * 20 ）
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);

        this.mappedFile = new DefaultMappedFile(fileName, fileTotalSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        // 500 0000
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public int getFileSize() {
        return this.fileTotalSize;
    }

    public void load() {
        this.indexHeader.load();
    }

    public void shutdown() {
        this.flush();
        UtilAll.cleanBuffer(this.mappedByteBuffer);
    }

    /**
     * 数据强制刷到磁盘、释放内存映射
     */
    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            // 更新 索引文件头
            this.indexHeader.updateByteBuffer();
            // 将缓冲区中的数据写入到物理硬盘，保证数据的持久化。
            this.mappedByteBuffer.force();
            // 释放内存映射
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        // 释放 映射内存空间
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * @param key  topic + "#" + uniqKey
     * @param phyOffset 消息在 commit log 的物理偏移量
     * @param storeTimestamp 存储时间戳
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 判断文件是否写满
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 获取 索引 key 的哈希值
            int keyHash = indexKeyHashMethod(key);
            // 哈希值与槽位取余，计算槽位
            int slotPos = keyHash % this.hashSlotNum;
            // 计算在消息在索引文件中，索引的相对位置
            // todo：存在冲突怎么办？ 在计算放入数据的位置 absIndexPos 时，依次写到文件最后的，即使发生冲突也是顺序写
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                // 获取 槽位的值
                // todo ： 槽位的值什么时候更新的？
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

                // 槽位值 小于 invalidIndex，默认 0
                // 或者 槽位值 大于 索引数，默认从 1 开始，
                /**
                 *
                 *  invalidIndex 的值是不变的，indexCount 每增加一个消息 +1
                 *  slotValue 是一个槽位的值，默认从 0 开始
                 *  slotValue 永远不会 大于 indexCount
                 *  slotValue 只有第一次放的消息的时候，slotValue <= invalidIndex
                 *  当后续的消息 再次计算出这个槽位，slotValue
                 *
                 *  invalidIndex 这个值实际上是作为判断标识，统计索引文件中索引个数的字段  hashSlotCount 是否增加
                 */
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 存储时间 与 索引文件中第一条消息的存储时间的差值
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                // 转换为秒单位，向下取整
                timeDiff = timeDiff / 1000;


                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    // 应对文件刚创建，初始值为0，为第一条消息的 timeDiff 赋值
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    // 索引文件的消息时间差值过大，情况猜测：当索引文件长时间没有消息插入
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    //  消息存储时间 在文件上个消息保存的时间之前
                    timeDiff = 0;
                }

                // 计算 消息 在索引文件中的相对位置
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                // 写入消息的 20字节索引信息
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 当第一放入消息 索引时，更新开始物理偏移量、开始时间戳
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }


                if (invalidIndex == slotValue) {
                    // 增加索引文件请求头中  hashSlotCount 中数据量
                    this.indexHeader.incHashSlotCount();
                }
                // 设置 增加 索引文件存储的消息数量
                this.indexHeader.incIndexCount();
                // 设置 索引文件最后一个消息的 commit log 的绝对偏移量
                this.indexHeader.setEndPhyOffset(phyOffset);
                // 设置 索引文件最后一个消息的 存储时间
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * 获取 索引 key 的哈希值
     * @param key
     * @return
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp();
        result = result || end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp();
        return result;
    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = timeRead >= begin && timeRead <= end;

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}
