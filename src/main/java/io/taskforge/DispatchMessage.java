package io.taskforge;

/** A notification to claim one task generation, never an instruction to skip database validation. */
public record DispatchMessage(String eventId, String taskId, long expectedToken) { }
