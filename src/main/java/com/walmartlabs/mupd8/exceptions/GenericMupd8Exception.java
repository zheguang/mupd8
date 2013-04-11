package com.walmartlabs.mupd8.exceptions;

public class GenericMupd8Exception extends RuntimeException {
  public GenericMupd8Exception() {}

  public GenericMupd8Exception(String message, Throwable cause) {
    super(message, cause);
  }

  public GenericMupd8Exception(String message) {
    super(message);
  }

  public GenericMupd8Exception(Throwable cause) {
    super(cause);
  }
}
