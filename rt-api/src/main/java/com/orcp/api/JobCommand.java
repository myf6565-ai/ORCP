package com.orcp.api;

public record JobCommand(String jobJar, String jobName, String[] args) {
}
