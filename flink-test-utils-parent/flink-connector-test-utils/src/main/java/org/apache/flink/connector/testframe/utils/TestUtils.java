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

package org.apache.flink.connector.testframe.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.List;
import java.util.stream.Collectors;

/** Test utils. */
public class TestUtils {
    public static final long METRIC_FETCHER_UPDATE_INTERVAL_MS = 1000L;
    public static final long SLOT_REQUEST_TIMEOUT_MS = 10000L;
    public static final long HEARTBEAT_TIMEOUT_MS = 5000L;
    public static final long HEARTBEAT_INTERVAL_MS = 1000L;

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
}
