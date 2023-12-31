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

import de.flapdoodle.embed.process.io.NamedOutputStreamProcessor;
import de.flapdoodle.embed.process.io.ProcessOutput;

public class Loggers {

    public enum LoggingStyle {
        FILE, CONSOLE, NONE
    }

    public static ProcessOutput file(String logFile, String encoding) {
        FileOutputStreamProcessor file = new FileOutputStreamProcessor(logFile, encoding);

        return ProcessOutput.builder()
              .output(new NamedOutputStreamProcessor("[mongod output]", file))
              .error(new NamedOutputStreamProcessor("[mongod error]", file))
              .commands(new NamedOutputStreamProcessor("[mongod commands]", file))
              .build();
    }

    public static ProcessOutput console() {
        return ProcessOutput.namedConsole("[mongod]");
    }

    public static ProcessOutput none() {
        return ProcessOutput.silent();
    }
}
