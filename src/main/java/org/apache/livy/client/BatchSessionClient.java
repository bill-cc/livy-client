package org.apache.livy.client;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.apache.livy.entity.BatchBody;
import org.apache.livy.entity.BatchInfo;
import org.apache.livy.entity.BatchList;
import org.apache.livy.entity.BatchLog;
import org.apache.livy.entity.BatchState;
import org.apache.livy.exception.LivyException;

/**
 * Livy batch session client, this class implemented all
 * restful API
 * 
 * @author Bill Cheng
 *
 */
@Slf4j
public class BatchSessionClient {
	/**
	 * batch list
	 */
	private static final String GET_BATCHES = "/batches?from={from}&size={size}";
	private static final String POST_BATCHES = "/batches";
	/**
	 * batch info
	 */
	private static final String GET_BATCH_INFO = "/batches/{batchId}";
	private static final String DEL_BATCH_INFO = "/batches/{batchId}";
	/**
	 * batch state
	 */
	private static final String GET_BATCH_STATE = "/batches/{batchId}/state";
	/**
	 * batch log
	 */
	private static final String GET_BATCH_LOG = "/batches/{batchId}/log?from={from}&size={size}";
	
	private RestTemplate restTemplate;
    private String livyURL;

    /**
     * Constructor with livy URL
     * 
     * @param livyURL
     * @param livyConf
     * @param restartDeadSession
     * @param currentSessionId
     */
    public BatchSessionClient(String livyURL) {
    	this.restTemplate = new RestTemplate();
        this.livyURL = livyURL;
    }

    /**
     * Get batch session list
     * 
     * @param from
     * @param size
     * @return
     */
    public BatchList getBatches(int from, int size) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("from", from);
    	params.put("size", size);
    	String url = livyURL + GET_BATCHES;
    	ResponseEntity<BatchList> obj = 
    			restTemplate.getForEntity(url, BatchList.class, params);
    	if((obj != null) && (obj.getBody() != null)) {
    		log.info("Got batch list with total number : {}", obj.getBody().getTotal());
    	}
		return obj.getBody();
    }
    
    /**
     * Create a new batch
     * 
     * @param from
     * @param size
     * @return
     */
    public BatchInfo createBatch(BatchBody batchBody) {
    	String url = livyURL + POST_BATCHES;
    	ResponseEntity<BatchInfo> obj = 
    			restTemplate.postForEntity(url, batchBody, BatchInfo.class);
    	if(obj != null) {
    		log.info("Got batch session with id : {}", obj.getBody().getId());
    	}
		return obj.getBody();
    }
    
    /**
     * Get batch session by id
     * 
     * @param from
     * @param size
     * @return
     */
    public BatchInfo getBatchInfo(int batchId) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("batchId", batchId);
    	String url = livyURL + GET_BATCH_INFO;
    	ResponseEntity<BatchInfo> obj = 
    			restTemplate.getForEntity(url, BatchInfo.class, params);
    	if(obj != null) {
    		log.info("Got batch with id : {}", obj.getBody().getId());
    	}
		return obj.getBody();
    }
    
    /**
     * Get batch session state by id
     * 
     * @param from
     * @param size
     * @return
     */
    public BatchState getBatchState(int batchId) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("batchId", batchId);
    	String url = livyURL + GET_BATCH_STATE;
    	ResponseEntity<BatchState> obj = 
    			restTemplate.getForEntity(url, BatchState.class, params);
    	if(obj != null) {
    		log.info("Got batch session state with id : {}", obj.getBody().getId());
    	}
		return obj.getBody();
    }
    
    /**
     * Delete session by id
     * 
     * @param from
     * @param size
     * @return
     */
    public void closeBatch(int batchId) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("batchId", batchId);
    	String url = livyURL + DEL_BATCH_INFO;
    	restTemplate.delete(url, params);
    }
    
    /**
     * Get batch session log by id
     * 
     * @param from
     * @param size
     * @return
     */
    public BatchLog getBatchLog(int batchId, int from, int size) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("batchId", batchId);
    	params.put("from", from);
    	params.put("size", size);
    	String url = livyURL + GET_BATCH_LOG;
    	ResponseEntity<BatchLog> obj = 
    			restTemplate.getForEntity(url, BatchLog.class, params);
    	if(obj != null) {
    		log.info("Got batch log with id={}, size={}", 
    				 obj.getBody().getId(), obj.getBody().getSize());
    	}
		return obj.getBody();
    }
    
    /**
     * Open batch session by ID
     */
    public BatchInfo openBatch(int batchId) throws LivyException {
    	BatchInfo resVal = null;
        if(batchId > -1) {
        	resVal = getBatchInfo(batchId);
            log.info("Open batch session successfully with batch ID: {}", batchId);
        }
        return resVal;
    }
    
    /**
     * 
     * Synchronous submit batch
     * 
     * @param batchBody
     * @return
     * @throws LivyException
     */
    public BatchInfo awaitSubmitBatch(BatchBody batchBody) throws LivyException {
    	return submitBatch(batchBody, true);
    }
    
    /**
     * 
     * Asynchronous submit batch
     * 
     * @param batchBody
     * @return
     * @throws LivyException
     */
    public BatchInfo asyncSubmitBatch(BatchBody batchBody) throws LivyException {
    	return submitBatch(batchBody, false);
    }
    
    /**
     * Submit batch
     * 
     * @param batchBody
     * @return
     * @throws LivyException
     */
    private BatchInfo submitBatch(BatchBody batchBody, boolean sync) throws LivyException {
    	BatchInfo batchInfo = null;
        try {
        	batchInfo = createBatch(batchBody);
        	log.info("Submit livy batch successfully with batch ID: {}", batchInfo.getId());
        	if(sync) {
	            do{
	                Thread.sleep(1000L);
	                batchInfo = getBatchInfo(batchInfo.getId());
	                if (batchInfo.isRunning()) {
	                    log.info("Batch {} are running, Application Id: {}, Tracking URL: {}", 
	                    		batchInfo.getId(), batchInfo.getAppId(), 
	                    		batchInfo.getAppInfo().get("sparkUiUrl"));
	                } else {
	                    log.info("Batch {} is in state {}, appId {}", 
	                    		 batchInfo.getId(), batchInfo.getState().getState(), batchInfo.getAppId());
	                }
	            } while(!batchInfo.isFinished());
	            if(batchInfo != null) {
		            log.info("Batch " + batchInfo.getId() + " is finished, " + 
		           		 	 "batch state is " + batchInfo.getState() + 
		           		 	 ", appId: " + batchInfo.getAppId() + 
		           		 	 ", log:\n" + getBatchLog(batchInfo.getId(), 0, 10000).logs());
	            }
        	}
        }catch (Exception e) {
            log.error("Error when creating livy batch", e);
            throw new LivyException(e);
        }
        return batchInfo;
    }
}
