/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;

/**
 * Default implementation of {@link LogStore} for Hadoop {@link FileSystem} implementations.
 */
public abstract class HadoopFileSystemLogStore extends LogStore {

    public HadoopFileSystemLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    public CloseableIterator<String> read(Path path, Configuration hadoopConf) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);
        FSDataInputStream stream = fs.open(path);
        Reader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        return new LineCloseableIterator(reader);
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);
        if (!fs.exists(path.getParent())) {
            throw new FileNotFoundException(
                String.format("No such file or directory: %s", path.getParent())
            );
        }
        FileStatus[] files = fs.listStatus(path.getParent());
        return Arrays.stream(files)
            .filter(f -> f.getPath().getName().compareTo(path.getName()) >= 0)
            .sorted(Comparator.comparing(o -> o.getPath().getName()))
            .iterator();
    }

    @Override
    public Path resolvePathOnPhysicalStorage(
            Path path,
            Configuration hadoopConf) throws IOException {
        return path.getFileSystem(hadoopConf).makeQualified(path);
    }

    /**
     * Create a temporary path (to be used as a copy) for the input {@code path}
     */
    protected Path createTempPath(Path path) {
        return new Path(
            path.getParent(),
            String.format(".%s.%s.tmp", path.getName(), UUID.randomUUID())
        );
    }

    /**
     * An internal write implementation that uses FileSystem.rename()
     *
     * This implementation should only be used for the underlying file systems that support atomic
     * renames, e.g, Azure is OK but HDFS is not.

     */
    protected void writeWithRename(
            Path path, Iterator<String> actions, boolean overwrite, Configuration hadoopConf
    ) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);

        if (!fs.exists(path.getParent())) {
            throw new FileNotFoundException(
                    String.format("No such file or directory: %s", path.getParent())
            );
        }

        if (overwrite) {
            try (FSDataOutputStream stream = fs.create(path, true)) {
                while (actions.hasNext()) {
                    stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
                }
            }
        } else {
            if (fs.exists(path)) {
                throw new FileAlreadyExistsException(path.toString());
            }

            Path tempPath = createTempPath(path);
            FSDataOutputStream stream = fs.create(tempPath);
            boolean renameDone = false;
            boolean streamClosed = false;

            try {
                while (actions.hasNext()) {
                    stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
                }

                stream.close();
                streamClosed = true;

                if (fs.rename(tempPath, path)) {
                    renameDone = true;
                } else {
                    if (fs.exists(path)) {
                        throw new FileAlreadyExistsException(path.toString());
                    } else {
                        throw new IllegalStateException(String.format("Cannot rename %s to %s", tempPath, path));
                    }
                }
            } finally {
                if (!streamClosed) {
                    stream.close();
                }
                if (!renameDone) {
                    fs.delete(tempPath, false);
                }
            }
        }
    }
}
