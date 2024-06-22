#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#===========================================================================================
# Java Environment Setting
# Java 环境配置：寻找 java home
#===========================================================================================
error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

find_java_home()
{
    case "`uname`" in
        Darwin)

          if [ -n "$JAVA_HOME" ]; then
            JAVA_HOME=$JAVA_HOME
            return
          fi
            JAVA_HOME=$(/usr/libexec/java_home)
        ;;
        *)
            JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
        ;;
    esac
}
# 执行
find_java_home

# -e 判断文件是否存在      前半段判断为真之后，才会执行后半段语句
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
# 没有找到后，调用 error_exit 函数报错，打印语句
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${BASE_DIR}/lib/*:${CLASSPATH}

#===========================================================================================
# JVM Configuration
# jvm 配置
#===========================================================================================
# The RAMDisk initializing size in MB on Darwin OS for gc-log
DIR_SIZE_IN_MB=600

choose_gc_log_directory()
{
    case "`uname`" in
        Darwin)
            if [ ! -d "/Volumes/RAMDisk" ]; then
                # create ram disk on Darwin systems as gc-log directory
                DEV=`hdiutil attach -nomount ram://$((2 * 1024 * DIR_SIZE_IN_MB))` > /dev/null
                diskutil eraseVolume HFS+ RAMDisk ${DEV} > /dev/null
                echo "Create RAMDisk /Volumes/RAMDisk for gc logging on Darwin OS."
            fi
            GC_LOG_DIR="/Volumes/RAMDisk"
        ;;
        *)
            # check if /dev/shm exists on other systems
            if [ -d "/dev/shm" ]; then
                GC_LOG_DIR="/dev/shm"
            else
                GC_LOG_DIR=${BASE_DIR}
            fi
        ;;
    esac
}

choose_gc_options()
{
    # 查看 java 版本
    JAVA_MAJOR_VERSION=$("$JAVA" -version 2>&1 | head -1 | cut -d'"' -f2 | sed 's/^1\.//' | cut -d'.' -f1)
    # jdk 小于 8：GC 年轻代使用UseParNewGC，老年代使用 CMS
    if [ -z "$JAVA_MAJOR_VERSION" ] || [ "$JAVA_MAJOR_VERSION" -lt "8" ] ; then
      JAVA_OPT="${JAVA_OPT} -Xmn4g -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:-UseParNewGC"
    # 大于 8：使用 G1 垃圾回收器
    else
      JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:G1ReservePercent=25 -XX:InitiatingHeapOccupancyPercent=30 -XX:SoftRefLRUPolicyMSPerMB=0"
    fi

    # jdk 小于 9，输出 GC详情到 rmq_srv_gc_%p_%t.log 文件中
    if [ -z "$JAVA_MAJOR_VERSION" ] || [ "$JAVA_MAJOR_VERSION" -lt "9" ] ; then
      JAVA_OPT="${JAVA_OPT} -verbose:gc -Xloggc:${}/rmq_srv_gc_%p_%t.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintAdaptiveSizePolicy"
      JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=30m"
    # 大于 9 ，配置GC输出日志，指定region大小
    else
      JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:G1ReservePercent=25 -XX:InitiatingHeapOccupancyPercent=30 -XX:SoftRefLRUPolicyMSPerMB=0"
      JAVA_OPT="${JAVA_OPT} -Xlog:gc*:file=${GC_LOG_DIR}/rmq_srv_gc_%p_%t.log:time,tags:filecount=5,filesize=30M"
    fi
}

choose_gc_log_directory
# 指定内存大小
JAVA_OPT="${JAVA_OPT} -server -Xms8g -Xmx8g"
choose_gc_options
# 频繁跑出异常场景下，提高异常抛出性能
JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow"
# 操作系统内存管理配置：进程启动分配内存时，加载所有预分配内存页，防止页中断，提高系统运行效率
JAVA_OPT="${JAVA_OPT} -XX:+AlwaysPreTouch"
# 最大对外内存
JAVA_OPT="${JAVA_OPT} -XX:MaxDirectMemorySize=15g"
# 不使用大页内存管理功能、关闭jvm偏向锁
JAVA_OPT="${JAVA_OPT} -XX:-UseLargePages -XX:-UseBiasedLocking"
#JAVA_OPT="${JAVA_OPT} -Xdebug -Xrunjdwp:transport=dt_socket,address=9555,server=y,suspend=n"
JAVA_OPT="${JAVA_OPT} ${JAVA_OPT_EXT}"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"

numactl --interleave=all pwd > /dev/null 2>&1
if [ $? -eq 0 ]
then
	if [ -z "$RMQ_NUMA_NODE" ] ; then
		numactl --interleave=all $JAVA ${JAVA_OPT} $@
	else
		numactl --cpunodebind=$RMQ_NUMA_NODE --membind=$RMQ_NUMA_NODE $JAVA ${JAVA_OPT} $@
	fi
else
	"$JAVA" ${JAVA_OPT} $@
fi
