package com.mariuspaavel.javaserializationlib;

/**
*An exception that is thrown when the data that is deserialized is invalid.
*/
public class SerializationException extends RuntimeException {
	SerializationException(String message) {
		super(message);
	}
}
