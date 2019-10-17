package org.apache.livy.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.NonNull;

public enum EStatementState {
    /**
     * Statement is enqueued but execution hasn't started
     */
    WAITING("waiting"),

    /**
     * Statement is currently running
     */
    RUNNING("running"),

    /**
     * Statement has a response ready
     */
    AVAILABLE("available"),

    /**
     * Statement failed
     */
    ERROR("error"),

    /**
     * Statement is being cancelling
     */
    CANCELLING("cancelling"),

    /**
     * Statement is cancelled
     */
    CANCELLED("cancelled"),
    ;

    @JsonValue
    private String state;

    EStatementState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }

    @JsonCreator
    public static EStatementState getStatementState(@NonNull String state) {
        for(EStatementState statementState : values()) {
            if(statementState.getState().equals(state)) {
                return statementState;
            }
        }
        throw new IllegalArgumentException("no this state:" + state);
    }
}
