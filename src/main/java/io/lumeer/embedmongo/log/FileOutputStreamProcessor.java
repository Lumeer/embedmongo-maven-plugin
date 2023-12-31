/*
 * Lumeer: Modern Data Definition and Processing Platform
 *
 * Copyright (C) since 2017 Lumeer.io, s.r.o. and/or its affiliates.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.lumeer.embedmongo.log;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import de.flapdoodle.embed.process.io.StreamProcessor;

public class FileOutputStreamProcessor implements StreamProcessor {

    private static OutputStreamWriter stream;

    private String logFile;
    private String encoding;

    public FileOutputStreamProcessor(String logFile, String encoding) {
        setLogFile(logFile);
        setEncoding(encoding);
    }

    @Override
    public synchronized void process(String block) {
        try {

            if (stream == null) {
                stream = new OutputStreamWriter(new FileOutputStream(logFile), encoding);
            }

            stream.write(block);
            stream.flush();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onProcessed() {
        process("\n");
    }

    private void setLogFile(String logFile) {
        if (logFile == null || logFile.trim().length() == 0) {
            throw new IllegalArgumentException("no logFile given");
        }
        this.logFile = logFile;
    }

    private void setEncoding(String encoding) {
        if (encoding == null || encoding.trim().length() == 0) {
            throw new IllegalArgumentException("no encoding given");
        }
        this.encoding = encoding;
    }
}
