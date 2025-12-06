package com.rdd.spark;

public class LogEntry {
    private String ip;
    private String date;
    private String method;
    private String resource;
    private int code;
    private int size;

    public LogEntry(String ip, String date, String method, String resource, int code, int size) {
        this.ip = ip;
        this.date = date;
        this.method = method;
        this.resource = resource;
        this.code = code;
        this.size = size;
    }

    public String getIp() { return ip; }
    public String getDate() { return date; }
    public String getMethod() { return method; }
    public String getResource() { return resource; }
    public int getCode() { return code; }
    public int getSize() { return size; }
}
