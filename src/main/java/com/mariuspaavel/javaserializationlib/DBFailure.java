package com.mariuspaavel.serializationlib;

public class DBFailure extends RuntimeException {
	DBFailure(String cause){
		super(cause);
	}
}
