package org.apache.livy.client;

import org.apache.livy.entity.CompletionBody;
import org.apache.livy.entity.CompletionList;
import org.apache.livy.entity.SessionBody;
import org.apache.livy.entity.SessionInfo;
import org.apache.livy.entity.SessionList;
import org.apache.livy.entity.SessionLog;
import org.apache.livy.entity.SessionState;
import org.apache.livy.entity.StatementBody;
import org.apache.livy.entity.StatementInfo;
import org.apache.livy.entity.StatementList;
import org.apache.livy.enums.ESessionKind;
import org.apache.livy.exception.LivyException;

import cn.hutool.core.thread.ThreadUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 
 * Livy interactive session client.
 * 
 * @author Bill Cheng
 *
 */
@Slf4j
public class InteractiveSessionClient {
	/**
	 * session list
	 */
	private static final String GET_SESSIONS = "/sessions?from={from}&size={size}";
	private static final String POST_SESSIONS = "/sessions";
	/**
	 * session info
	 */
	private static final String GET_SESSION_INFO = "/sessions/{sessionId}";
	private static final String DEL_SESSION_INFO = "/sessions/{sessionId}";
	/**
	 * session state
	 */
	private static final String GET_SESSION_STATE = "/sessions/{sessionId}/state";
	/**
	 * session log
	 */
	private static final String GET_SESSION_LOG = "/sessions/{sessionId}/log?from={from}&size={size}";
	/**
	 * session statements
	 */
	private static final String GET_SESSIONS_STATEMENTS = "/sessions/{sessionId}/statements";
	private static final String POST_SESSIONS_STATEMENTS = "/sessions/{sessionId}/statements";
	/**
	 * statement info
	 */
	private static final String GET_STATEMENT_INFO = "/sessions/{sessionId}/statements/{statementId}";
	/**
	 * statement cancel
	 */
	private static final String POST_STATEMENT_CANCEL = "/sessions/{sessionId}/statements/{statementId}/cancel";
	/**
	 * session completion
	 */
	private static final String POST_SESSION_COMPLETION = "/sessions/{sessionId}/completion";
	
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
    public InteractiveSessionClient(String livyURL) {
    	this.restTemplate = new RestTemplate();
        this.livyURL = livyURL;
    }

    /**
     * Get session list
     * 
     * @param from
     * @param size
     * @return
     */
    public SessionList getSessions(int from, int size) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("from", from);
    	params.put("size", size);
    	String url = livyURL + GET_SESSIONS;
    	ResponseEntity<SessionList> obj = 
    			restTemplate.getForEntity(url, SessionList.class, params);
    	if((obj != null) && (obj.getBody() != null)) {
    		log.info("Got sessions with total number : {}", obj.getBody().getTotal());
    	}
		return obj.getBody();
    }
    
    /**
     * Create a new session
     * 
     * @param from
     * @param size
     * @return
     */
    public SessionInfo createSession(SessionBody sessionBody) {
    	String url = livyURL + POST_SESSIONS;
    	ResponseEntity<SessionInfo> obj = 
    			restTemplate.postForEntity(url, sessionBody, SessionInfo.class);
    	if(obj != null) {
    		log.info("Got session with id : {}", obj.getBody().getId());
    	}
		return obj.getBody();
    }
    
    /**
     * Get session by id
     * 
     * @param from
     * @param size
     * @return
     */
    public SessionInfo getSessionInfo(int sessionId) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("sessionId", sessionId);
    	String url = livyURL + GET_SESSION_INFO;
    	ResponseEntity<SessionInfo> obj = 
    			restTemplate.getForEntity(url, SessionInfo.class, params);
    	if(obj != null) {
    		log.info("Got session with id : {}", obj.getBody().getId());
    	}
		return obj.getBody();
    }
    
    /**
     * Get session state by id
     * 
     * @param from
     * @param size
     * @return
     */
    public SessionState getSessionState(int sessionId) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("sessionId", sessionId);
    	String url = livyURL + GET_SESSION_STATE;
    	ResponseEntity<SessionState> obj = 
    			restTemplate.getForEntity(url, SessionState.class, params);
    	if(obj != null) {
    		log.info("Got session state with id : {}", obj.getBody().getId());
    	}
		return obj.getBody();
    }
    
    /**
     * Close session by id
     * 
     * @param from
     * @param size
     * @return
     */
    public void closeSession(int sessionId) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("sessionId", sessionId);
    	String url = livyURL + DEL_SESSION_INFO;
    	restTemplate.delete(url, params);
    }
    
    /**
     * Get session log by id
     * 
     * @param from
     * @param size
     * @return
     */
    public SessionLog getSessionLog(int sessionId, int from, int size) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("sessionId", sessionId);
    	params.put("from", from);
    	params.put("size", size);
    	String url = livyURL + GET_SESSION_LOG;
    	ResponseEntity<SessionLog> obj = 
    			restTemplate.getForEntity(url, SessionLog.class, params);
    	if(obj != null) {
    		log.info("Got session state with id : {}", obj.getBody().getId());
    	}
		return obj.getBody();
    }
    
    /**
     * Get session statement list by id
     * 
     * @param from
     * @param size
     * @return
     */
    public StatementList getStatements(int sessionId) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("sessionId", sessionId);
    	String url = livyURL + GET_SESSIONS_STATEMENTS;
    	ResponseEntity<StatementList> obj = 
    			restTemplate.getForEntity(url, StatementList.class, params);
    	if(obj != null) {
    		log.info("Got session state with size : {}", obj.getBody().getStatements().size());
    	}
		return obj.getBody();
    }
    
    /**
     * Execute a statement
     * 
     * @param from
     * @param size
     * @return
     */
    public StatementInfo executeStatement(int sessionId, String code, ESessionKind kind) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("sessionId", sessionId);
    	String url = livyURL + POST_SESSIONS_STATEMENTS;
    	StatementBody statementBody = new StatementBody();
    	statementBody.setCode(code);
    	statementBody.setKind(kind);
    	ResponseEntity<StatementInfo> obj = 
    			restTemplate.postForEntity(url, statementBody, StatementInfo.class, params);
    	if(obj != null) {
    		log.info("Got session state with id : {}", obj.getBody().getId());
    	}
		return obj.getBody();
    }
    
    /**
     * Get statement info
     * 
     * @param from
     * @param size
     * @return
     */
    public StatementInfo getStatement(int sessionId, int statementId) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("sessionId", sessionId);
    	params.put("statementId", statementId);
    	String url = livyURL + GET_STATEMENT_INFO;
    	ResponseEntity<StatementInfo> obj = 
    			restTemplate.getForEntity(url, StatementInfo.class, params);
    	if(obj != null) {
    		log.info("Got session state with id : {}", obj.getBody().getId());
    	}
		return obj.getBody();
    }
    
    /**
     * Cancel a statement
     * 
     * @param from
     * @param size
     * @return
     */
    public boolean cancelStatement(int sessionId, int statementId) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("sessionId", sessionId);
    	params.put("statementId", statementId);
    	String url = livyURL + POST_STATEMENT_CANCEL;
    	ResponseEntity<String> obj = 
    			restTemplate.postForEntity(url, null, String.class, params);
    	if(obj != null) {
    		log.info("Cancel session state with msg : {}", obj.getBody());
    	}
		return "cancelled".equals(obj.getBody()) ? true : false;
    }
    
    /**
     * Run session completion
     * 
     * @param from
     * @param size
     * @return
     */
    public CompletionList runSessionCompletion(
    		int sessionId, String code, String cursor, ESessionKind kind) {
    	Map<String, Object> params = new HashMap<>();
    	params.put("sessionId", sessionId);
    	String url = livyURL + POST_SESSION_COMPLETION;
    	CompletionBody completionBody = new CompletionBody();
    	completionBody.setCode(code);
    	completionBody.setCursor(cursor);
    	completionBody.setKind(kind);
    	ResponseEntity<CompletionList> obj = 
    			restTemplate.postForEntity(url, completionBody, CompletionList.class, params);
    	if(obj != null) {
    		log.info("Got session completion with size : {}", obj.getBody().getCandidates().size());
    	}
		return obj.getBody();
    }

    /**
     * Sync create session 
     */
    public SessionInfo awaitOpenSession(SessionBody sessionBody) throws LivyException {
        try {
        	SessionInfo sessionInfo = createSession(sessionBody);
            log.info("Session is creating");
            String msg;
            do{
                if(sessionInfo.isReady()) {
                    return sessionInfo;
                }
                Thread.sleep(1000L);
                sessionInfo = getSessionInfo(sessionInfo.getId());
                if (sessionInfo.isReady()) {
                    log.info("Session {} has been established.", sessionInfo.getId());
                    log.info("Application Id: {}, Tracking URL: {}", 
                    		sessionInfo.getAppId(), sessionInfo.getAppInfo().get("sparkUiUrl"));
                } else {
                    log.info("Session {} is in state {}, appId {}", 
                    		sessionInfo.getId(), sessionInfo.getState().getState(), sessionInfo.getAppId());
                }
                if(sessionInfo.isFinished()) {
                    msg = "Create Session " + sessionInfo.getId() + 
                    	  " is finished, appId: " + sessionInfo.getAppId() + ", log:\n" +
                          getSessionLog(sessionInfo.getId(), 0, 1000).logs();
                    throw new LivyException(msg);
                }
            } while(!sessionInfo.isFinished());
            msg = "Session " + sessionInfo.getId() + 
            	  " start error, log:\n" + 
            	  getSessionLog(sessionInfo.getId(), 0, 1000).logs();
            throw new LivyException(msg);
        }catch (Exception e) {
            log.error("Error when creating livy session", e);
            throw new LivyException(e);
        }
    }
    
    /**
     * Sync execute statement
     */
    public StatementInfo awaitExecuteStatement(
    		int sessionId, String code, ESessionKind kind) throws LivyException {
        return awaitExecuteStatement(sessionId, code, kind, 1000L, null);
    }

    /**
     * @param code SQL code
     * @param durationMillis  wait in
     * @param handleStatementInfo  handle function
     * @return
     * @throws LivyException
     */
    public StatementInfo awaitExecuteStatement(
    		int sessionId, String code, ESessionKind kind, 
    		long durationMillis, Consumer<StatementInfo> handleStatementInfo) 
    		throws LivyException {
        StatementInfo stmtInfo = executeStatement(sessionId, code, kind);
        try {
            while(true) {
                if(stmtInfo.isAvailable()) {
                    break;
                }
                ThreadUtil.sleep(durationMillis);
                stmtInfo = getStatement(sessionId, stmtInfo.getId());
                if(Objects.nonNull(handleStatementInfo)) {
                    handleStatementInfo.accept(stmtInfo);
                }
            }
        } catch (Exception e) {
            log.error("awaitExecuteStatement, code:{}, e:", code, e);
            throw new LivyException(e);
        }

        if(stmtInfo.getOutput().isError()) {
            throw new LivyException("failed execute code:" + stmtInfo.getOutput().getData());
        }
        return stmtInfo;
    }
}
