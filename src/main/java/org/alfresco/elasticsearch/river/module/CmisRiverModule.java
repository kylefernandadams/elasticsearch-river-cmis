package org.alfresco.elasticsearch.river.module;

import org.alfresco.elasticsearch.river.CmisRiver;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class CmisRiverModule extends AbstractModule{

	@Override
	protected void configure() {
        bind(River.class).to(CmisRiver.class).asEagerSingleton();
	}
}
