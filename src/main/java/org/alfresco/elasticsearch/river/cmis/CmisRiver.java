package org.alfresco.elasticsearch.river.cmis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.chemistry.opencmis.client.api.Document;
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.Property;
import org.apache.chemistry.opencmis.client.api.QueryResult;
import org.apache.chemistry.opencmis.client.api.Repository;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.threadpool.ThreadPool;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class CmisRiver extends AbstractRiverComponent implements River{
	
	private final ThreadPool threadPool;
    private final Client client;
    
    private String cmisUrl = "http://localhost:8080/alfresco/api/-default-/public/cmis/versions/1.1/atom";
    private String username = "admin";
    private String password = "admin";
    private String network = "-default-";
    private String cmisQuery = "SELECT * FROM cmis:document";
    
    private final String indexName;
    private final String typeName;
    private String mapping = null;
    private final int bulkSize;
    private final int maxConcurrentBulk;
    private final TimeValue bulkFlushInterval;
    private volatile BulkProcessor bulkProcessor;
    
    private Session session = null;

    @SuppressWarnings("unchecked")
	@Inject
    public CmisRiver(RiverName riverName, RiverSettings riverSettings, Client client, ThreadPool threadPool) {
		super(riverName, riverSettings);
		this.client = client;
        this.threadPool = threadPool;
        
        logger.debug("Instantiating CMIS River...");
        if(riverSettings.settings().containsKey("cmis")){
            Map<String, Object> alfrescoSettings = (Map<String, Object>) riverSettings.settings().get("alfresco");
            
            if(alfrescoSettings.containsKey("on-prem")){
                Map<String, Object> onPremSettings = (Map<String, Object>) alfrescoSettings.get("on-prem");
                
                if(onPremSettings.containsKey("cmisUrl")){
                	cmisUrl = XContentMapValues.nodeStringValue(onPremSettings.get("cmisUrl"), cmisUrl);
                }
                if(onPremSettings.containsKey("username")){
                	username = XContentMapValues.nodeStringValue(onPremSettings.get("username"), username);
                }
                if(onPremSettings.containsKey("password")){
                	password = XContentMapValues.nodeStringValue(onPremSettings.get("password"), password);
                }
                if(onPremSettings.containsKey("cmisQuery")){
                	cmisQuery = XContentMapValues.nodeStringValue(onPremSettings.get("cmisQuery"), cmisQuery);
                }
            }
            
			this.getCmisSession(username, password, cmisUrl);
        }
        
        if (riverSettings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "status");
            mapping = XContentMapValues.nodeStringValue(indexSettings.get("mapping"), mapping);

            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            bulkFlushInterval = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(
                    indexSettings.get("flush_interval"), "5s"), TimeValue.timeValueSeconds(5));
            maxConcurrentBulk = XContentMapValues.nodeIntegerValue(indexSettings.get("max_concurrent_bulk"), 1);
        } else {
            indexName = riverName.name();
            typeName = "status";
            bulkSize = 100;
            maxConcurrentBulk = 1;
            bulkFlushInterval = TimeValue.timeValueSeconds(5);
        }
        
        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.info("Going to execute new bulk composed of {} actions", request.numberOfActions());
            }

            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.info("Executed bulk composed of {} actions", request.numberOfActions());
                
                if (response.hasFailures()) {
                    logger.warn("There was failures while executing bulk", response.buildFailureMessage());
                    if (logger.isDebugEnabled()) {
                        for (BulkItemResponse item : response.getItems()) {
                            if (item.isFailed()) {
                                logger.debug("Error for {}/{}/{} for {} operation: {}", item.getIndex(),
                                        item.getType(), item.getId(), item.getOpType(), item.getFailureMessage());
                            }
                        }
                    }
                }
            }

            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.warn("Error executing bulk", failure);
            }
        })
        .setBulkActions(bulkSize)
        .setConcurrentRequests(maxConcurrentBulk)
        .setFlushInterval(bulkFlushInterval)
        .build();
	}

	public void start() {
		logger.debug("Starting CmisRiver...");

		try {
			client.admin().indices().prepareCreate(indexName).execute().actionGet();
		} 
		catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
		}

		
		try{
			ItemIterable<QueryResult> queryResults = session.query(cmisQuery, false);
			for (QueryResult queryResult : queryResults) {
				String objectId = String.valueOf(queryResult.getPropertyById(PropertyIds.OBJECT_ID).getValues().get(0));
				Document document = (Document) session.getObject(session.createObjectId(objectId));
				
				List<Property<?>> propertyList = document.getProperties();
				Gson gson = new GsonBuilder().setPrettyPrinting().create();
				
				IndexRequest indexRequest = Requests.indexRequest(indexName).type(typeName).id(document.getId());
                String source = gson.toJson(propertyList);
                
                indexRequest.source(source);
                bulkProcessor.add(indexRequest);
                
                logger.info("Import from CMIS repository complete");                
			}
		}
		catch(Exception e){
			logger.error("Failed to start CMIS River", e);
			bulkProcessor.close();
		}
	}
	
	public void close() {
		logger.debug("Closing CMIS River...");
		bulkProcessor.close();	
	}
	
	public Session getCmisSession(String username, String password, String cmisUrl){
		
		try{
			// create a session
	        SessionFactory factory = SessionFactoryImpl.newInstance();
	        Map<String, String> parameterMap = new HashMap<String, String>();
	        parameterMap.put(SessionParameter.USER, username);
	        parameterMap.put(SessionParameter.PASSWORD, password);
	        parameterMap.put(SessionParameter.ATOMPUB_URL, cmisUrl);
	        parameterMap.put(SessionParameter.BINDING_TYPE,
	                BindingType.ATOMPUB.value());
	        // Use the first repository
	        List<Repository> repositories = factory.getRepositories(parameterMap);
	        session = repositories.get(0).createSession();	        
	        session.getDefaultContext().setCacheEnabled(false);
	        logger.debug("Successfully retrieved session for repo: " + session.getRepositoryInfo().toString());
		}
		catch(Exception e){
			logger.error("Failed to get Cmis Session", e);
		}
		return session;
	}
}