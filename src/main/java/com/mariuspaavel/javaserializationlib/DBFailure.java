package com.mariuspaavel.javaserializationlib;

public class DBFailure extends RuntimeException {
	DBFailure(String cause){
		super(cause);
	}
}
