package org.alfresco.elasticsearch.river.cmis.entity;

import java.util.List;

import org.apache.chemistry.opencmis.commons.data.PropertyData;

public class CmisObject {
	
	public String objectId;
	public List<PropertyData<?>> propertyData;
	public List<CmisProperty> propertyList;
//	public Map<String, Object> propertyMap = new HashMap<String, Object>();
	
	public String getObjectId() {
		return objectId;
	}
	public void setObjectId(String objectId) {
		this.objectId = objectId;
	}
	public List<PropertyData<?>> getPropertyData() {
		return propertyData;
	}
	public void setPropertyData(List<PropertyData<?>> propertyData) {
		this.propertyData = propertyData;
	}
	public List<CmisProperty> getPropertyList() {
		return propertyList;
	}
	public void setPropertyList(List<CmisProperty> propertyList) {
		this.propertyList = propertyList;
	}

	
	
}
