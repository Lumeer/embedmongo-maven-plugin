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

import de.flapdoodle.net.ProxyFactory;
import io.lumeer.embedmongo.log.Loggers;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import de.flapdoodle.embed.mongo.commands.MongodArguments;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.embed.mongo.types.DatabaseDir;
import de.flapdoodle.embed.mongo.types.DistributionBaseUrl;
import de.flapdoodle.embed.process.config.DownloadConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.settings.Settings;

import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.process.io.ProcessOutput;
import de.flapdoodle.embed.process.transitions.DownloadPackage;
import de.flapdoodle.embed.process.transitions.ImmutableDownloadPackage;
import de.flapdoodle.reverse.StateID;
import de.flapdoodle.reverse.Transition;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.Transitions;
import de.flapdoodle.reverse.transitions.Start;

/**
 * When invoked, this goal starts an instance of mongo. The required binaries
 * are downloaded if no mongo release is found in <code>~/.embedmongo</code>.
 * 
 * @see <a
 *      href="http://github.com/flapdoodle-oss/embedmongo.flapdoodle.de">http://github.com/flapdoodle-oss/embedmongo.flapdoodle.de</a>
 */
@Mojo(name="start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST)
public class StartMojo extends AbstractEmbeddedMongoMojo {

    private static final String PACKAGE_NAME = StartMojo.class.getPackage().getName();
    public static final String MONGOD_CONTEXT_PROPERTY_NAME = PACKAGE_NAME + ".mongod";

    @Override
    protected void savePortToProjectProperties(int port) {
        super.savePortToProjectProperties(port);
    }

    /**
     * The location of a directory that will hold the MongoDB data files.
     * 
     * @since 0.1.0
     */
    @Parameter(property = "embedmongo.databaseDirectory")
    private File databaseDirectory;

    /**
     * An IP address for the MongoDB instance to be bound to during its
     * execution.
     * 
     * @since 0.1.4
     */
    @Parameter(property = "embedmongo.bindIp")
    private String bindIp;

    /**
     * @since 0.1.3
     */
    @Parameter(property = "embedmongo.logging", defaultValue = "console")
    private String logging;

    /**
     * @since 0.1.7
     */
    @Parameter(property = "embedmongo.logFile", defaultValue = "embedmongo.log")
    private String logFile;

    /**
     * @since 0.1.7
     */
    @Parameter(property = "embedmongo.logFileEncoding", defaultValue = "utf-8")
    private String logFileEncoding;

    /**
     * The base URL to be used when downloading MongoDB
     * 
     * @since 0.1.10
     */
    @Parameter(property = "embedmongo.downloadPath", defaultValue = "http://fastdl.mongodb.org")
    private String downloadPath;

    /**
     * Should authorization be enabled for MongoDB
     */
    @Parameter(property = "embedmongo.authEnabled", defaultValue = "false")
    private boolean authEnabled;

    /**
     * The path for the UNIX socket
     * @since 0.3.5
     */
    @Parameter(property = "embedmongo.unixSocketPrefix")
    private String unixSocketPrefix;

    @Parameter(property = "embedmongo.journal", defaultValue = "false")
    private boolean journal;
    
    /**
     * The storageEngine which shall be used
     * 
     * @since 0.3.4
     */
    @Parameter(property = "embedmongo.storageEngine")
    private String storageEngine;

    @Parameter( defaultValue = "${settings}", readonly = true )
    protected Settings settings;

    @Override
    protected void onSkip() {
        getLog().debug("skip=true, not starting embedmongo");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void executeStart() throws MojoExecutionException, MojoFailureException {
        var b = Mongod.builder()
              .processOutput(Start.to(ProcessOutput.class).initializedWith(getOutputConfig()));

        if (StringUtils.isNotEmpty(downloadPath)) {
            b.distributionBaseUrl(getDownloadPath());
            b.downloadPackage(getProxyFactory(settings));
        }

        if (databaseDirectory != null) {
            b.databaseDir(Start.to(DatabaseDir.class).initializedWith(DatabaseDir.of(databaseDirectory.toPath())));
        }

        var args = MongodArguments.defaults()
              .withArgs(getMongodArgs())
              .withAuth(authEnabled)
              .withUseNoJournal(!journal);

        if (storageEngine != null) {
            args.withStorageEngine(storageEngine);
        }

        b.mongodArguments(Start.to(MongodArguments.class).initializedWith(args));

        if (bindIp == null) {
            bindIp = InetAddress.getLoopbackAddress().getHostAddress();
        }

        int port = getPort();
        if (isRandomPort()) {
            port = NetworkUtils.allocateRandomPort();
        }
        savePortToProjectProperties(port);

        b.net(Start.to(Net.class).initializedWith(Net.of(bindIp, port, NetworkUtils.localhostIsIPv6())));

        final Thread mongoThread = new Thread(() -> {
            Mongod mongod = b.build();

            Transitions transitions = mongod.transitions(getVersion());
            try (TransitionWalker.ReachedState<RunningMongodProcess> running = transitions.walker()
                  .initState(StateID.of(RunningMongodProcess.class))) {

                getPluginContext().put(MONGOD_CONTEXT_PROPERTY_NAME, running);

                getLog().info("Mongod successfully started.");

                while (running.current().isAlive()) {
                    try {
                        TimeUnit.MINUTES.sleep(1);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } catch (Exception e) {
                getLog().error("Unable to start the mongod.", e);
            }
        });
        mongoThread.setDaemon(true);
        mongoThread.start();

        try {
            if (isWait()) mongoThread.join();
        } catch (InterruptedException e) {
            getLog().info("Mongod got interrupted.");
        }
    }

    private Map<String, String> getMongodArgs() {
        Map<String, String> mongodArgs = new HashMap<>();

        if (System.getProperty("os.name").toLowerCase().indexOf("win") == -1 
            && this.unixSocketPrefix != null && !this.unixSocketPrefix.isEmpty()) {
            mongodArgs.put("unixSocketPrefix", this.unixSocketPrefix);
        }

        return mongodArgs;
    }

    private ProcessOutput getOutputConfig() throws MojoFailureException {

        Loggers.LoggingStyle loggingStyle = Loggers.LoggingStyle.valueOf(logging.toUpperCase());

        switch (loggingStyle) {
            case CONSOLE:
                return Loggers.console();
            case FILE:
                return Loggers.file(logFile, logFileEncoding);
            case NONE:
                return Loggers.none();
            default:
                throw new MojoFailureException("Unexpected logging style encountered: \"" + logging + "\" -> " +
                        loggingStyle);
        }

    }

    private Transition<DistributionBaseUrl> getDownloadPath() {
        return Start.to(DistributionBaseUrl.class)
              .initializedWith(DistributionBaseUrl.of(downloadPath));
    }

    public DownloadPackage getProxyFactory(Settings settings) {
        URI downloadUri = URI.create(downloadPath);
        final String downloadHost = downloadUri.getHost();
        final String downloadProto = downloadUri.getScheme();
        ImmutableDownloadPackage dp = DownloadPackage.withDefaults();

        if (settings.getProxies() != null) {
            for (org.apache.maven.settings.Proxy proxy : settings.getProxies()) {
                if (proxy.isActive()
                      && equalsIgnoreCase(proxy.getProtocol(), downloadProto)
                      && !contains(proxy.getNonProxyHosts(), downloadHost)) {
                    dp.withDownloadConfig(DownloadConfig.defaults().withProxyFactory(
                        ProxyFactory.of(proxy.getHost(), proxy.getPort())));
                }
            }
        }

        return dp;
    }

    private String getDataDirectory() {
        if (databaseDirectory != null) {
            return databaseDirectory.getAbsolutePath();
        } else {
            return null;
        }
    }

}
