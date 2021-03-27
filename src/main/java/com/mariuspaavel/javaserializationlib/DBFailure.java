package com.mariuspaavel.javaserializationlib;

/**
An exception that is thrown when a database encounters a fatal problem when doing an operation.
*/
public class DBFailure extends RuntimeException {
	DBFailure(String cause){
		super(cause);
	}
}
