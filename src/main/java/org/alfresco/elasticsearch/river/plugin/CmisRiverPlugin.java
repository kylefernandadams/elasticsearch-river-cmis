package org.alfresco.elasticsearch.river.plugin;

import org.alfresco.elasticsearch.river.CmisRiverModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class CmisRiverPlugin extends AbstractPlugin{

	@Inject
	public CmisRiverPlugin(){
	}
	
	public String description() {
		return "river-cmis";
	}

	public String name() {
		return "Alfresco CMIS River Plugin";
	}

	public void onModule(RiversModule module) {
        module.registerRiver("cmis", CmisRiverModule.class);
    }
}
