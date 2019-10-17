package org.apache.livy.enums;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.NonNull;

public enum ESessionKind {

    /**
     * Interactive Scala Spark session
     */
    SPARK("spark"),

    /**
     * Interactive Python Spark session
     */
    PYSPARK("pyspark"),

    /**
     * Interactive R Spark session
     */
    SPARKR("sparkr"),

    /**
     * Interactive SQL Spark session
     */
    SQL("sql"),
    ;

    @JsonValue
    private String kind;

    ESessionKind(String kind) {
        this.kind = kind;
    }

    public String getKind() {
        return kind;
    }

    @JsonCreator
    public static ESessionKind getSessionState(@NonNull String kind) {
        for(ESessionKind sessionKind : values()) {
            if(sessionKind.getKind().equals(kind)) {
                return sessionKind;
            }
        }
        throw new IllegalArgumentException("no this kind:" + kind);
    }
}
