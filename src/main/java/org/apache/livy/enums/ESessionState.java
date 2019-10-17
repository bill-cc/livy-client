package org.apache.livy.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.NonNull;

public enum ESessionState {
    /**
     * Session has not been started
     */
    NOT_STARTED("not_started"),

    /**
     * Session is starting
     */
    STARTING("starting"),
    
    /**
     * batch session is running
     */
    RUNNING("running"),

    /**
     * Session is waiting for input
     */
    IDLE("idle"),

    /**
     * Session is executing a statement
     */
    BUSY("busy"),

    /**
     * Session is shutting down
     */
    SHUTTING_DOWN("shutting_down"),

    /**
     * Session errored out
     */
    ERROR("error"),

    /**
     * Session has exited
     */
    DEAD("dead"),

    /**
     * Session has been killed
     */
    KILLED("killed"),

    /**
     * Session is successfully stopped
     */
    SUCCESS("success"),
    ;

    @JsonValue
    private String state;

    ESessionState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }

    @JsonCreator
    public static ESessionState getSessionState(@NonNull String state) {
        for(ESessionState sessionState : values()) {
            if(sessionState.getState().equals(state)) {
                return sessionState;
            }
        }
        throw new IllegalArgumentException("no this state:" + state);
    }
}
