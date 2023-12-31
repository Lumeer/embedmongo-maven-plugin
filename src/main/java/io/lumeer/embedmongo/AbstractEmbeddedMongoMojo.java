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

import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import de.flapdoodle.embed.mongo.distribution.Version;

/**
 * Created by pablo on 28/03/15.
 */
public abstract class AbstractEmbeddedMongoMojo extends AbstractMojo {
    @Parameter(property = "embedmongo.skip", defaultValue = "false")
    private boolean skip;

    /**
     * The port MongoDB should run on.
     *
     * @since 0.1.0
     */
    @Parameter(property = "embedmongo.port", defaultValue = "27017")
    private int port;

    /**
     * Whether a random free port should be used for MongoDB instead of the one
     * specified by {@code port}. If {@code randomPort} is {@code true}, the
     * random port chosen will be available in the Maven project property
     * {@code embedmongo.port}.
     *
     * @since 0.1.8
     */
    @Parameter(property = "embedmongo.randomPort", defaultValue = "false")
    private boolean randomPort;

    /**
     * The version of MongoDB to run e.g. 2.1.1, 1.6 v1.8.2, V2_0_4,
     *
     * @since 0.1.0
     */
    @Parameter(property = "embedmongo.version", defaultValue = "2.2.1")
    private String version;

    /**
     * Block immediately and wait until MongoDB is explicitly stopped (eg:
     * {@literal <ctrl-c>}). This option makes this goal similar in spirit to
     * something like jetty:run, useful for interactive debugging.
     *
     * @since 0.1.2
     */
    @Parameter(property = "embedmongo.wait", defaultValue = "false")
    private boolean wait;

    @Parameter( defaultValue = "${project}", readonly = true )
    protected MavenProject project;

    public AbstractEmbeddedMongoMojo() {
    }

    AbstractEmbeddedMongoMojo(int port) {
        this.port = port;
    }

    @Override
    public final void execute() throws MojoExecutionException, MojoFailureException {
        if(skip) {
            onSkip();
        } else {
            executeStart();
        }
    }

    protected void onSkip() {
        // Nothing to do, this is just to allow do things if mojo is skipped
    }

    protected Version getVersion() {
        String versionEnumName = this.version.toUpperCase().replaceAll("\\.", "_");

        if (versionEnumName.charAt(0) != 'V') {
            versionEnumName = "V" + versionEnumName;
        }

        try {
            return Version.valueOf(versionEnumName);
        } catch (IllegalArgumentException e) {
            getLog().warn("Unrecognised MongoDB version '" + this.version + "', this might be a new version that we don't yet know about. Attempting download anyway...");
            throw e;
        }
    }

    protected de.flapdoodle.embed.process.distribution.Version getDistributionVersion() {
        return de.flapdoodle.embed.process.distribution.Version.of(this.version);
    }

    protected Integer getPort() {
        String portStr = project.getProperties().getProperty("embedmongo.port");

        if(StringUtils.isNotBlank(portStr)){
            return Integer.valueOf(portStr);
        }else{
            return port;
        }
    }

    public abstract void executeStart() throws MojoExecutionException, MojoFailureException;

    /**
     * Saves port to the {@link MavenProject#getProperties()} (with the property
     * name {@code embedmongo.port}) to allow others (plugins, tests, etc) to
     * find the randomly allocated port.
     *
     * @param port the port.
     */
    protected void savePortToProjectProperties(int port) {
        project.getProperties().put("embedmongo.port", String.valueOf(port));
    }

    public boolean isSkip() {
        return skip;
    }

    public boolean isRandomPort() {
        return randomPort;
    }

    public boolean isWait() {
        return wait;
    }

    public MavenProject getProject() {
        return project;
    }

}
