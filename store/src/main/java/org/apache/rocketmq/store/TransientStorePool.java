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
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class TransientStorePool {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 默认值：5
     */
    private final int poolSize;
    /**
     * 默认值：1G
     */
    private final int fileSize;
    /**
     * 添加时机：
     * 开启 TransientStorePool 后，默认创建 5个 ByteBuffer，
     * 每个 ByteBuffer 创建时候，调用 mlock 方法对内存进行处理，添加到双队列中
     *
     * 重新添加时机：
     * 以 commit log 为例，CommitRealTimeService 服务会刷新 将写入位置复制给提交位置，
     * 此时如果 mappedFile 提交位置已经和文件大小相同，整个文件刷新完成，会清空 ByteBuffer，并再次添加到队头
     */
    private final Deque<ByteBuffer> availableBuffers;

    /**
     * 是否真正提交 标识
     * 当开启缓冲池后，commit log 的 CommitRealTimeService 服务，
     * isRealCommit = false： 刷新提交位置 不是向 fileChanel 写数据，而是将 mappedFile 的 committedPosition 设置为 writeposition。
     * isRealCommit = true：向 fileChannel 中写入数据
     */
    private volatile boolean isRealCommit = true;

    public TransientStorePool(final int poolSize, final int fileSize) {
        this.poolSize = poolSize;
        this.fileSize = fileSize;
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // 通过该方法，锁住内存，防止内存回收
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        return availableBuffers.size();
    }

    public boolean isRealCommit() {
        return isRealCommit;
    }

    public void setRealCommit(boolean realCommit) {
        isRealCommit = realCommit;
    }
}
