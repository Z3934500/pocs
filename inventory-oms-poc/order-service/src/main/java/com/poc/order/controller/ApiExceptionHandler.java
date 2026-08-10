package com.poc.order.controller;

import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

/** Maps order validation and state conflicts to HTTP Problem Details. */
@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(IllegalArgumentException.class) ProblemDetail badRequest(IllegalArgumentException ex) { return problem(HttpStatus.BAD_REQUEST, ex); }
    @ExceptionHandler(IllegalStateException.class) ProblemDetail conflict(IllegalStateException ex) { return problem(HttpStatus.CONFLICT, ex); }
    private static ProblemDetail problem(HttpStatus status, RuntimeException ex) { ProblemDetail detail=ProblemDetail.forStatusAndDetail(status, ex.getMessage()); detail.setTitle(status.getReasonPhrase()); return detail; }
}