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
package io.lumeer.embedmongo;

import static org.apache.commons.lang3.StringUtils.*;

import java.io.File;

public class ImportDataConfig {
    private String database;
    private String collection;
    private String file;
    private Boolean dropOnImport = true;
    private Boolean upsertOnImport = true;
    private long timeout = 200000;

    public ImportDataConfig() {
    }

    public ImportDataConfig(String database, String collection, String file, Boolean dropOnImport, Boolean upsertOnImport, long timeout) {
        this.database = database;
        this.collection = collection;
        this.file = file;
        this.dropOnImport = dropOnImport;
        this.upsertOnImport = upsertOnImport;
        this.timeout = timeout;
    }

    public String getDatabase() {

        return database;
    }

    public String getCollection() {
        if (isBlank(collection)) {
            return substringBeforeLast(substringAfterLast(this.file, File.separator), ".");
        } else {        
            return collection;
        }
    }

    public String getFile() {
        return file;
    }

    public Boolean getDropOnImport() {
        return dropOnImport;
    }

    public Boolean getUpsertOnImport() {
        return upsertOnImport;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public String toString() {
        return "ImportDataConfig{" +
                "database='" + database + '\'' +
                ", collection='" + collection + '\'' +
                ", file='" + file + '\'' +
                ", dropOnImport=" + dropOnImport +
                ", upsertOnImport=" + upsertOnImport +
                ", timeout=" + timeout +
                '}';
    }
}
