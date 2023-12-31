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

import de.flapdoodle.embed.mongo.commands.MongoImportArguments;
import de.flapdoodle.embed.mongo.commands.ServerAddress;
import de.flapdoodle.embed.mongo.transitions.ExecutedMongoImportProcess;
import de.flapdoodle.embed.mongo.transitions.MongoImport;
import de.flapdoodle.embed.mongo.transitions.MongoImportProcessArguments;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.reverse.StateID;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.Transitions;
import de.flapdoodle.reverse.transitions.Start;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.IOException;
import java.net.InetAddress;

@Mojo(name="mongo-import", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST)
public class MongoImportMojo extends AbstractEmbeddedMongoMojo {
    @Parameter
    private ImportDataConfig[] imports;

    @Parameter(property = "embedmongo.defaultImportDatabase")
    private String defaultImportDatabase;

    @Parameter(property = "embedmongo.parallel", defaultValue = "false")
    private Boolean parallel;

    @Override
    public void executeStart() throws MojoExecutionException, MojoFailureException {
        try {
            sendImportScript();
        } catch (Exception e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }

    }

    private void sendImportScript() throws IOException, InterruptedException, MojoExecutionException {
        if(imports == null || imports.length == 0) {
            getLog().error("No imports found, check your configuration");

            return;
        }

        getLog().info("Default import database: " + defaultImportDatabase);

        for(ImportDataConfig importData: imports) {

            getLog().info("Import " + importData);

            verify(importData);
            String database = importData.getDatabase();

            if (StringUtils.isBlank(database)) {
                database = defaultImportDatabase;
            }
            MongoImportArguments arguments = MongoImportArguments.builder()
                  .databaseName(database)
                  .collectionName(importData.getCollection())
                  .importFile(importData.getFile())
                  .isJsonArray(true)
                  .upsertDocuments(importData.getUpsertOnImport())
                  .dropCollection(importData.getDropOnImport())
                  .build();

            MongoImportProcessArguments pa = MongoImportProcessArguments.builder()
                  .build();

            try (TransitionWalker.ReachedState<RunningMongodProcess> mongoD = Mongod.instance()
                  .transitions(getVersion())
                  .addAll(Start.to(ServerAddress.class).initializedWith(ServerAddress.of(InetAddress.getLocalHost(), getPort())))
                  .walker()
                  .initState(StateID.of(RunningMongodProcess.class))) {

                Transitions mongoImportTransitions = MongoImport.instance()
                      .transitions(getVersion())
                      .replace(Start.to(MongoImportArguments.class).initializedWith(arguments))
                      .addAll(Start.to(ServerAddress.class).initializedWith(mongoD.current().getServerAddress()));

                try (TransitionWalker.ReachedState<ExecutedMongoImportProcess> executed = mongoImportTransitions.walker()
                      .initState(StateID.of(ExecutedMongoImportProcess.class))) {
                    getLog().info("Import return code: " + executed.current().returnCode());
                }
            }
        }
    }


    private void verify(ImportDataConfig config) {
        Validate.notBlank(config.getFile(), "Import file is required\n\n" +
                "<imports>\n" +
                "\t<import>\n" +
                "\t\t<file>[my file]</file>\n" +
                "...");
        Validate.isTrue(StringUtils.isNotBlank(defaultImportDatabase) || StringUtils.isNotBlank(config.getDatabase()), "Database is required you can either define a defaultImportDatabase or a <database> on import tags");
    }

}
