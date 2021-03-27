package com.mariuspaavel.javaserializationlib;

/**
*An exception that is thrown when a database encounters a non-fatal problem during an operation.
*/

public class DBException extends RuntimeException {
	DBException(String cause){
		super(cause);
	}
}
