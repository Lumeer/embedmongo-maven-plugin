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

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.reverse.TransitionWalker;

import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

/**
 * When invoked, this goal stops an instance of mojo that was started by this
 * plugin.
 */
@Mojo(name="stop", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST)
public class StopMojo extends AbstractEmbeddedMongoMojo {

    @Override
    public void executeStart() throws MojoExecutionException, MojoFailureException {
        TransitionWalker.ReachedState<RunningMongodProcess> mongod = (TransitionWalker.ReachedState<RunningMongodProcess>) getPluginContext().get(StartMojo.MONGOD_CONTEXT_PROPERTY_NAME);

        System.out.printf("@@@@@@@@@@@@@@@@@@@ žádost o zastavení");

        if (mongod != null && mongod.current() != null && mongod.current().isAlive()) {
            mongod.current().stop();
        } else {
            throw new MojoFailureException("No mongod process found, it appears embedmongo:start was not called");
        }
    }
}
