package org.alfresco.elasticsearch.plugin.river.cmis;

import org.alfresco.elasticsearch.river.cmis.CmisRiverModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class CmisRiverPlugin extends AbstractPlugin{

	@Inject
	public CmisRiverPlugin(){
	}
	
	public String name() {
		return "river-cmis";
	}
	
	public String description() {
		return "Alfresco CMIS River Plugin";
	}

	public void onModule(RiversModule module) {
        module.registerRiver("cmis", CmisRiverModule.class);
    }
}
