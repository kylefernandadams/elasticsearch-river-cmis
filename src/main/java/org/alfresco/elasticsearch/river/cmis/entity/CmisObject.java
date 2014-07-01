package org.alfresco.elasticsearch.river.cmis.entity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CmisObject {

	public Map<String, List<?>> propertyMap = new HashMap<String, List<?>>();

	public Map<String, List<?>> getPropertyMap() {
		return propertyMap;
	}

	public void setPropertyMap(Map<String, List<?>> propertyMap) {
		this.propertyMap = propertyMap;
	}
}
