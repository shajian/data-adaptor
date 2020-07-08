package com.qcm.task.com;

public enum TaskType {
    es(1),
    mongo(2),
    arango(4),
    redis(8);

    private TaskType(int v) {
        value = v;
    }
    private int value;
    public int getValue() { return value; }
}
