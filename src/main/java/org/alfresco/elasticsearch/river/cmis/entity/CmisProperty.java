package org.alfresco.elasticsearch.river.cmis.entity;

import java.util.List;

public class CmisProperty {
	public String id;
	public String name;
	public List<?> values;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<?> getValues() {
		return values;
	}
	public void setValues(List<?> values) {
		this.values = values;
	}

}
