package com.mariuspaavel.javaserializationlib;

/**
* All objects that are designed to be the contents of a database must implement this interface.
* The implementors of this inteface msut provide a Metadata class, that contains the primary key id of the database line thet describes the object.
*/

public interface DBObject {
	/**
	Provide the DBObjectMeta class that contains the primary key.
	*/
	public DBObjectMeta getMeta();
}
