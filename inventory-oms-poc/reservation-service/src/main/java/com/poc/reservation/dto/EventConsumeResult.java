package com.poc.reservation.dto;

public record EventConsumeResult(String eventId, boolean processed, String message) {
}