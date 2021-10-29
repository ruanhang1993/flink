/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.test.common.utils;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connectors.test.common.external.sink.SinkDataReader;

import org.junit.jupiter.api.function.Executable;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE;
import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Test utils. */
public class TestUtils {
    public static File newFolder(Path path) throws IOException {
        Path tempPath = Files.createTempDirectory(path, "testing-framework", new FileAttribute[0]);
        return tempPath.toFile();
    }

    public static void waitAndExecute(Executable task, long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            // do nothing
        }
        try {
            task.execute();
        } catch (Throwable e) {
            throw new IllegalStateException("Error to execute task.", e);
        }
    }

    public static <T> List<T> getResultData(
            SinkDataReader<T> reader,
            List<T> expected,
            int retryTimes,
            DeliveryGuarantee semantic) {
        long timeoutMs = 1000L;
        int retryIndex = 0;
        if (EXACTLY_ONCE.equals(semantic)) {
            List<T> result = new ArrayList<>();
            while (retryIndex++ < retryTimes && result.size() < expected.size()) {
                result.addAll(reader.poll(timeoutMs));
            }
            return result;
        } else if (AT_LEAST_ONCE.equals(semantic)) {
            List<T> result = new ArrayList<>();
            while (retryIndex++ < retryTimes && !containSameVal(expected, result, semantic)) {
                result.addAll(reader.poll(timeoutMs));
            }
            return result;
        }
        throw new IllegalStateException(
                String.format("%s delivery guarantee doesn't support test.", semantic.name()));
    }

    public static <T> boolean containSameVal(
            List<T> expected, List<T> result, DeliveryGuarantee semantic) {
        checkNotNull(expected);
        checkNotNull(result);

        Set<Integer> matchedIndex = new HashSet<>();
        if (EXACTLY_ONCE.equals(semantic) && expected.size() != result.size()) {
            return false;
        }
        for (T rowData0 : expected) {
            int before = matchedIndex.size();
            for (int i = 0; i < result.size(); i++) {
                if (matchedIndex.contains(i)) {
                    continue;
                }
                if (rowData0.equals(result.get(i))) {
                    matchedIndex.add(i);
                    break;
                }
            }
            if (before == matchedIndex.size()) {
                return false;
            }
        }
        return true;
    }

    public static void timeoutAssert(
            ExecutorService executorService, Runnable task, long time, TimeUnit timeUnit) {
        Future future = executorService.submit(task);
        try {
            future.get(time, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException("Test failed to get the result.", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Test failed with some exception.", e);
        } catch (TimeoutException e) {
            throw new RuntimeException(
                    String.format("Test timeout after %d %s.", time, timeUnit.name()), e);
        } finally {
            future.cancel(true);
        }
    }

    public static void deletePath(Path path) throws IOException {
        List<File> files =
                Files.walk(path)
                        .filter(p -> p != path)
                        .map(Path::toFile)
                        .collect(Collectors.toList());
        for (File file : files) {
            if (file.isDirectory()) {
                deletePath(file.toPath());
            } else {
                file.delete();
            }
        }
        Files.deleteIfExists(path);
    }

    public static boolean doubleEquals(double d0, double d1) {
        BigDecimal decimal0 = new BigDecimal(d0);
        BigDecimal decimal1 = new BigDecimal(d1);
        return decimal0.compareTo(decimal1) == 0;
    }
}
