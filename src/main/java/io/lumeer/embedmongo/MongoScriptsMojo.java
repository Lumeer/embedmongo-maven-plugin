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

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Scanner;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/**
 * When invoked, this goal connects to an instance of mongo and execute some
 * instructions to add data.
 *
 * You should use the same javascript syntax that you would use in the mongo
 * client.
 *
 */
@Mojo(name = "mongo-scripts", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST)
public class MongoScriptsMojo extends AbstractEmbeddedMongoMojo {

    /**
     * Folder that contains all scripts to execute.
     */
    @Parameter(property = "scriptsDirectory", required = true)
    private File scriptsDirectory;

    /**
     * Charset encoding to use for parsing the scripts.  If not assigned,
     * the underlying encoding of the operating system will be
     * used
     */
    @Parameter(property = "scriptCharsetEncoding", required = false)
    private String scriptCharsetEncoding;

    /**
     * The name of the database where data will be stored.
     */
    @Parameter(property = "databaseName", required = true)
    private String databaseName;

    public MongoScriptsMojo() {
    }

    MongoScriptsMojo(File scriptsDirectory, int port, String databaseName, String scriptCharsetEncoding) {
        super(port);
        this.scriptsDirectory = scriptsDirectory;
        this.databaseName = databaseName;
        this.scriptCharsetEncoding = scriptCharsetEncoding;
    }

    @Override
    public void executeStart() throws MojoExecutionException, MojoFailureException {
        MongoDatabase db = connectToMongoAndGetDatabase();

        if (scriptsDirectory.isDirectory()) {
            Scanner scanner = null;
            StringBuilder instructions = new StringBuilder();
            File[] files = scriptsDirectory.listFiles();

            if (files == null) {
                getLog().info("Can't read scripts directory: " + scriptsDirectory.getAbsolutePath());

            } else {
                getLog().info("Folder " + scriptsDirectory.getAbsolutePath() + " contains " + files.length + " file(s):");

                for (File file : files) {
                    if (file.isFile()) {
                        try {
                            if (scriptCharsetEncoding == null) {
                                scanner = new Scanner(file);
                            } else {
                                // no need to check encoding, the constructor throws
                                // an IllegalArgumentException if the charset cannot be determined
                                // from the provided value.
                                scanner = new Scanner(file, scriptCharsetEncoding);
                            }

                            while (scanner.hasNextLine()) {
                                instructions.append(scanner.nextLine()).append("\n");
                            }
                        } catch (FileNotFoundException e) {
                            throw new MojoExecutionException("Unable to find file with name '" + file.getName() + "'", e);
                        } catch (IllegalArgumentException e) {
                            throw new MojoExecutionException("Unable to determine charset encoding for provided charset '" + scriptCharsetEncoding + "'", e);
                        } finally {
                            if (scanner != null) {
                                scanner.close();
                            }
                        }
                        Document result;
                        try {
                            Bson command = new BsonDocument("eval", new BsonString("function() {" + instructions.toString() + "}"));
                            result = db.runCommand(command);
                        } catch (MongoException e) {
                            throw new MojoExecutionException("Unable to execute file with name '" + file.getName() + "'", e);
                        }
                        if (result.getInteger("ok") != 1) {
                            getLog().error("- file " + file.getName() + " parsed with error: " + result.getString("errmsg"));
                            throw new MojoExecutionException("Error while executing instructions from file '" + file.getName() + "': " + result.getString("errmsg"));
                        }
                        getLog().info("- file " + file.getName() + " parsed successfully");
                    }
                }
            }
            getLog().info("Data initialized with success");
        }
    }

    MongoDatabase connectToMongoAndGetDatabase() throws MojoExecutionException {
        if (databaseName == null || databaseName.trim().length() == 0) {
            throw new MojoExecutionException("Database name is missing");
        }

        ServerAddress addr = new ServerAddress(InetAddress.getLoopbackAddress(), getPort());

        MongoClient mongoClient = MongoClients.create("mongodb://" + addr);
        getLog().info("Connected to MongoDB");
        return mongoClient.getDatabase(databaseName);
    }
}
