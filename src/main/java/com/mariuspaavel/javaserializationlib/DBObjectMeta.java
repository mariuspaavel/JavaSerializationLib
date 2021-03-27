package com.mariuspaavel.javaserializationlib;

/**
*A class that contains the primary key id of the database object.
* All classes that are designed to be the contants of a database must implement the DBObject interface, thus provide the DBObjectMeta class.
*/
public class DBObjectMeta {
	private int id = -1;
	void setId(int id) {
		this.id = id;
	}
	public int getId() {
		return id;
	}
}
