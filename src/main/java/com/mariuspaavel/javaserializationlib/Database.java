package com.mariuspaavel.javaserializationlib;

import java.io.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import com.mariuspaavel.javautilities.*;


/**
*A SQLite database that saves special serializable objects.
* An object that is inserted to a database msut meet three conditions:
* 1) It must implement the DBObject interface.
* 2) It must be registrated to the Manager class
* 3) It's defining fields msut be marked with the S annotation
*/
public class Database {
		
	Connection c;
	
	private Map<String, Table> tables = new HashMap<String, Table>();
		
	void addTable(Table t){
		if(isopen)throw new RuntimeException("Cannot add table when database is already opened");
		
		tables.put(t.getName(), t);
	
	}
	
	private boolean isopen;

	/**
	*Ends the initialization phase of the database.
	* No more tables can be created after that.
	*/
	public void open() {
		if(isopen)return;
		c = createDB(getName());
		
		try {
			for(Table t : tables.values()) {
				t.open();
			}
		}catch(SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(String.format("Cannot open database %s", getName()));
		}
		
		isopen = true;
	}
	private Connection createDB(String name) {
		if(ds!=null)ds.println("Opening database " + getName());
		Connection c = null;
		try {
	    	 final Path datafolder = Paths.get(System.getProperty("user.home"), "data");
	    	 if (!Files.exists(datafolder))Files.createDirectory(datafolder);
			 Path dbPath = datafolder.resolve(name+".db"); 
			 
	         Class.forName("org.sqlite.JDBC");
	         c = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toString());
	      } catch ( SQLException | ClassNotFoundException | IOException e ) {
	         //System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	         throw new RuntimeException("Unable initiate database " + getName() + e.getClass().getName() + ": " + e.getMessage());
	      }
	      if(ds!=null)ds.println("Opened database " + getName() + " successfully");
	      return c;
	}
	private void close() {
		if(!isopen)return;
		try {
			c.close();
			if(ds!=null)ds.println("Closing database " + getName());
			isopen = false;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private Manager mg;
	private String name;	

	/**
	*Start the initialization phase of the database.
	* @param name The name of the database.
	* @param mg The manager class using which classes are serialized to blobs when needed.
	*/
	
	public Database(String name, Manager mg, PrintStream ds) {
		this.name = name;
		this.mg = mg;
		this.ds = ds;
	}
	
	public Database(String name, Manager mg){
		this(name, mg, null);
	}

	PrintStream ds;
	
	/**
	*Get the name of the database.
	*/
	public String getName() {
		return name;
	}
	
	
	
	public class Table <T extends DBObject> {
		private String name;
		
		/**
		*Get the name of the database
		*/
		public String getName() {
			return name;
		}
		
		private Class dataType;
		
		
		/**
		*Add a table to the database.
		* A table can only be added during the initialization phase.
		* A table constructor msut only be provided the name of the table and the datatype. The database class will take care of the rest (Table creation, opening etc.).
		* @param name The name of the table.
		* @param dataType the class of the object that this table contains. When a table is created with this datatype, it can only accept objects of this datatype 
		* (subclasses not included)
		*/
		public Table(String name, Class dataType) {
			if(Database.this.isopen) {
				throw new RuntimeException("Error: A new table cannot be opened when database is already open");
			}
			this.name = name;
			this.dataType = dataType;
			Database.this.addTable(this);
		}
		
		
		private boolean isopen = false;
		void open() throws SQLException {
			if(!tableExist())create();
			initId();
			isopen = true;
		}
		
		private void create() throws SQLException {
			StringBuilder sb = new StringBuilder();
			TreeMap<String, Field> fc = mg.getClassFields(dataType);
			
			sb.append("CREATE TABLE ");
			sb.append(name);
			sb.append(" (");
			sb.append("dbid INT PRIMARY KEY");

			
			Iterator<String> fi = fc.keySet().iterator();
			if(fi.hasNext())sb.append(", ");
			while(fi.hasNext()) {
				Field f = fc.get(fi.next());
				sb.append(f.getName());
				sb.append(" ");
				String decltype = null;
				Class fieldClass = f.getType();
				
				if(fieldClass.equals(Boolean.class) || fieldClass.equals(boolean.class)){
					sb.append("TINYINT");
				}
				if(fieldClass.equals(Byte.class) || fieldClass.equals(byte.class)) {
					sb.append("TINYINT");
				}
				else if(fieldClass.equals(Short.class) || fieldClass.equals(short.class)){
					sb.append("INT");
				}
				else if(fieldClass.equals(Integer.class) || fieldClass.equals(int.class)) {
					sb.append("INT");
				}
				else if(fieldClass.equals(Long.class) || fieldClass.equals(long.class)) {
					sb.append("BIGINT");
				}
				else if(fieldClass.equals(Float.class) || fieldClass.equals(float.class)){
					sb.append("REAL");
				}
				else if(fieldClass.equals(Double.class) || fieldClass.equals(double.class)){
					sb.append("REAL");
				}
				else if(fieldClass.equals(String.class)) {
					sb.append("TEXT");
				}
				else if(fieldClass.isInstance(List.class)) {
					sb.append("TEXT");
				}
				else if(fieldClass.equals(byte[].class)) {
					sb.append("BLOB");
				}
				else {
					sb.append("TEXT");
				}
					
				if(fi.hasNext())sb.append(", ");
			}
			sb.append(");");
			String sql = sb.toString();
			
			if(ds!=null)ds.println(sql);
			
			try(Statement stmt = c.createStatement()) {
				stmt.executeUpdate(sql);
			}
			
		}
		private boolean tableExist() throws SQLException {
		    boolean tExists = false;
		    try (ResultSet rs = c.getMetaData().getTables(null, null, name, null)) {
		        while (rs.next()) { 
		            String tName = rs.getString("TABLE_NAME");
		            if (tName != null && tName.equals(name)) {
		                tExists = true;
		                break;
		            }
		        }
		    }
		    return tExists;
		}
		private void initId(){
			String query = String.format("SELECT MAX(dbid) FROM %s;", name);
			if(ds!=null)ds.println(query);
			try(Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(query);) {

				if(ds!=null)ds.println("Getting max id");
				
				if(!rs.next()) {
					idCounter = 1;
					return;
				}
				int maxid = rs.getInt(1);

				idCounter = maxid+1;
				
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Cannot initiate id counter for database table " + name);
			}
			
		}
		
		
		
		
		private int idCounter;
		private int nextId() {
			return ++idCounter;
		}
		
		
		/**
		*Insert an object to the database.
		* @param o the object that is going to be inserted.
		*/
		public void insert(DBObject o) {
			if(!Database.this.isopen)Database.this.open();
			
			StringBuilder sb = new StringBuilder();
			sb.append("INSERT INTO ");
			sb.append(name);
			sb.append(" (");
			
			TreeMap<String, Field> fmap = mg.getClassFields(dataType);
			
			sb.append("dbid, ");
			
			Iterator<String> names = fmap.keySet().iterator();
			
			
			while(names.hasNext()) {
				sb.append(names.next());
				if(names.hasNext())sb.append(", ");
			}
			sb.append(") VALUES (");
			
			int id = nextId();
			o.getMeta().setId(id);
			sb.append(id);
			sb.append(", ");
			
			names = fmap.keySet().iterator();
			
			HashMap<Integer, byte[]> blobs = new HashMap<Integer, byte[]>();
			int blobIndex = 0;
			
			try {
				while(names.hasNext()) {
					Field f = fmap.get(names.next());
					
					Class fclass = f.getType();
					
					
					if(fclass.equals(boolean.class)){
						sb.append(f.getBoolean(o) ? "1" : "0");
					}
					else if(fclass.equals(byte.class)) {
						sb.append(f.getByte(o));
					}
					else if(fclass.equals(short.class)){
						sb.append(f.getShort(o));
					}
					else if(fclass.equals(int.class)) {
						sb.append(f.getInt(o));
					}
					else if(fclass.equals(long.class)) {
						sb.append(f.getLong(o));
					}
					else if(fclass.equals(float.class)){
						sb.append(f.getFloat(o));
					}
					else if(fclass.equals(double.class)){
						sb.append(f.getDouble(o));
					}
					else{
						Object value = f.get(o);
						if(value == null)sb.append("NULL");
						else if(fclass.equals(Byte.class) 
							|| fclass.equals(Short.class) 
							|| fclass.equals(Integer.class) 
							|| fclass.equals(Long.class)
							|| fclass.equals(Float.class)
							|| fclass.equals(Double.class)
							) {
							sb.append(f.get(o).toString());
						}
						else if(fclass.equals(Boolean.class)){
							boolean b = (Boolean)f.get(o);
							sb.append(b ? "1" : "0");
						}
						else if(fclass.equals(String.class)) {
							sb.append("\"");
							sb.append(f.get(o));
							sb.append("\"");
						}
						else if(fclass.equals(byte[].class)) {
							sb.append("?");
							blobs.put(blobIndex, (byte[])f.get(o));
							blobIndex++;
						}
						else if(fclass.isInstance(List.class)) {
							ByteArrayOutputStream stream = new ByteArrayOutputStream();
							mg.ObjectToBytes(f.get(o), stream);
							byte[] bytes = stream.toByteArray();
							sb.append("\"");
							sb.append(B64.encode(bytes));
							sb.append("\"");
						}
						else {
							ByteArrayOutputStream stream = new ByteArrayOutputStream();
							mg.ObjectToBytes(f.get(o), stream);
							byte[] bytes = stream.toByteArray();
							sb.append("\"");
							sb.append(B64.encode(bytes));
							sb.append("\"");
						}
					}
					if(names.hasNext())sb.append(", ");
				}
				sb.append(");");		
			
				String sql = sb.toString();
				
				if(ds != null)ds.println(sql);		
				
				if(blobs.keySet().size() == 0) {
					try(Statement stmt = c.createStatement()) {			
						stmt.executeUpdate(sql);
					}
				}
				else {
					try(PreparedStatement pstmt = c.prepareStatement(sql)) {
					   	
						for(Integer i : blobs.keySet()) {
							pstmt.setBytes(blobIndex, blobs.get(i));
						}
			        	
						pstmt.executeUpdate();
			      
			          		 if(ds!=null)ds.println("Uploaded blob data");
			            
					}
				}
			}catch(IllegalArgumentException | IllegalAccessException  | SQLException e) {
				e.printStackTrace();
				throw new DBException(e.getMessage());
			}
		}
		
		/**
		*Set 1 field of an object in a table.
		* @param id The primary key id of the object in that table.
		* @param fieldName The field to be changed.
		* @param value The new value for this field.
		*/
		public void setField(int id, String fieldName, Object value)  {
			if(!Database.this.isopen)Database.this.open();
				
			TreeMap<String, Field> fields = mg.getClassFields(dataType);
			
			Field f = fields.get(fieldName);
			if(f == null)throw new RuntimeException(String.format("Object %s doens't contain field %s", dataType.getName(), fieldName));
			
			StringBuilder sb = new StringBuilder();
			sb.append("UPDATE ");
			sb.append(name);
			sb.append(" SET ");
			sb.append(fieldName);
			sb.append("=");
	
			byte[] blob = null;
			
			try {
				
				Class fclass = f.getType();
				
				if(value == null)sb.append("NULL");
				else if(value instanceof String){
					sb.append("\"");
					sb.append(value.toString());
					sb.append("\"");
				}
				else if(fclass.isInstance(List.class)) {
					ByteArrayOutputStream stream = new ByteArrayOutputStream();
					mg.ObjectToBytes(f.get(value), stream);
					byte[] bytes = stream.toByteArray();
					sb.append("\"");
					sb.append(B64.encode(bytes));
					sb.append("\"");
				}
				else if(fclass.equals(byte[].class)) {
					sb.append("?");
					blob = (byte[])f.get(value);
				}
				else if(value instanceof Number){
					sb.append(value.toString());
				}
				else if(value instanceof Boolean){
					if((Boolean)value)sb.append(1);
					else sb.append(0);
				}
				else {
					ByteArrayOutputStream stream = new ByteArrayOutputStream();
					mg.ObjectToBytes(value, stream);
					byte[] bytes = stream.toByteArray();
					sb.append("\"");
					sb.append(B64.encode(bytes));
					sb.append("\"");
				}

				sb.append(" WHERE dbid=");
				sb.append(id);
				sb.append(";");
				
				String sql = sb.toString();
				
				if(ds!=null)ds.println(sql);
				
				
				if(blob == null) {
					try(Statement stmt = c.createStatement()){
						stmt.executeUpdate(sb.toString());
					}
				}else {
					try(PreparedStatement pstmt = c.prepareStatement(sql)){
						pstmt.executeQuery(sql);
						pstmt.setBytes(0, blob);
					}
				}
			}catch( SQLException | IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
				throw new DBException(e.getMessage());
			}
		}
		
		/**
		*Gets an object from the database given the primary key id contained in the DBObjectMeta class
		* @Param dbid The primary key id contained in the DBObjectMeta class.
		*/
		
		public T get(int dbid) {
			if(!Database.this.isopen)Database.this.open();
			String sql = String.format("SELECT * FROM %s WHERE dbid=%s;", name, dbid);
			
			if(ds != null)ds.println(sql);			
	
			try(Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)){
				if(!rs.next())return null;
				return readRS(rs);
			}catch(SQLException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException | IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
			
		}
		
		/**
		*Queries objects from the table that meet a specific contition.
		* The SQL query is formed as: "SELECT * FROM [table name] WHERE [contition string]."
		* @param condition the String that contains the standard SQL query after the "WHERE" keyword.
		* @return An ArrayList containing the query results.
		*/		

		public ArrayList<T> query(String condition)  {
			if(!Database.this.isopen)Database.this.open();
			
			ArrayList<T> output = new ArrayList<T>();
			String query = String.format("SELECT * FROM %s %s;", name, condition);
			if(ds!=null)ds.println(query);
			try(Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery( query);) {
				while(rs.next()) {
					output.add((T) readRS(rs));
				}
			}catch(SQLException | IOException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
				e.printStackTrace();
				throw new DBException(e.getMessage());
			}
			if(ds!=null)ds.println("QUERY RESULT:");
			if(ds!=null)ds.println("----------------------------------------------------");
			for(T t : output)if(ds!=null)ds.println(t);
			if(ds!=null)ds.println("----------------------------------------------------");
			return output;
		}
		
		public T queryFirst(String condition){
			if(!Database.this.isopen)Database.this.open();
			String query = String.format("SELECT * FROM %s %s;", name, condition);
			if(ds!=null)ds.println(query);
			try(Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery( query);) {
				if(rs.next()) {
					return (T)readRS(rs);
				}else return null;
			}catch(SQLException | IOException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
				e.printStackTrace();
				throw new DBException(e.getMessage());
			}
		}
		
		/**
		*Gets one specific field from an object in the table.
		* @param id The primary key id of the object in the database. =
		* @param fieldName The name of the field that is asked.
		* @return The content of the field as an obejct. if the field is a primitive, then it's wrapped.
		*/
		
		public Object getField(int id, String fieldName) {
			try(Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(String.format("SELECT %s FROM %s WHERE dbid=%d", name, fieldName, id));) {
				if(!rs.next())return null;
				Class ftype = mg.getClassFields(dataType).get(fieldName).getType();
				
				if(ftype.equals(boolean.class) || ftype.equals(Boolean.class)){
					return rs.getBoolean(fieldName);
				}
				else if(ftype.equals(byte.class) || ftype.equals(Byte.class)) {
					return rs.getByte(fieldName);
				}
				else if(ftype.equals(short.class) || ftype.equals(Short.class)){
					return (short)rs.getInt(fieldName);
				}
				else if(ftype.equals(int.class) || ftype.equals(Integer.class)) {
					return rs.getInt(fieldName);
				}
				else if(ftype.equals(long.class) || ftype.equals(Long.class)) {
					return rs.getLong(fieldName);
				}
				else if(ftype.equals(float.class) || ftype.equals(Float.class)) {
					return rs.getFloat(fieldName);
				}
				else if(ftype.equals(double.class) || ftype.equals(Double.class)) {
					return rs.getInt(fieldName);
				}
				else if(ftype.equals(byte[].class)) {
					return rs.getBytes(fieldName);
				}
				else if(ftype.isInstance(List.class)) {
					String b64 = rs.getString(name);
					if(b64==null)return null;
					byte[] bytes = B64.decode(b64);
					ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
					List l = (List)mg.bytesToObject(stream);
					return l;
				}
				else {
					String b64 = rs.getString(name);
					if(b64 == null)return null;
					byte[] bytes = B64.decode(b64);
					ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
					Object o = (Object)mg.bytesToObject(stream);
					return o;
				}

			}catch(SQLException | IllegalArgumentException | SecurityException e) {
				e.printStackTrace();
				throw new DBException(e.getMessage());
			}
		}
		
		
		private T readRS(ResultSet rs) throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException {
			TreeMap<String, Field> dTypeFields = mg.getClassFields(dataType);
			DBObject output = (DBObject)dataType.getDeclaredConstructor().newInstance();
			output.getMeta().setId(rs.getInt("dbid"));
			
			for(String name : dTypeFields.keySet()) {
				Field f = dTypeFields.get(name);
				Class ftype = f.getType();
				

				if(ftype.equals(boolean.class)){
					f.setBoolean(output, rs.getByte(name) != 0);
				}
				else if(ftype.equals(byte.class)) {
					f.setByte(output, rs.getByte(name));
				}
				else if(ftype.equals(short.class)){
					f.setShort(output, (short)rs.getInt(name));
				}
				else if(ftype.equals(int.class)) {
					f.setInt(output, rs.getInt(name));
				}
				else if(ftype.equals(long.class)) {
					f.setLong(output, rs.getLong(name));
				}
				else if(ftype.equals(float.class)){
					f.setFloat(output, rs.getFloat(name));
				}
				else if(ftype.equals(double.class)){
					f.setDouble(output, rs.getDouble(name));
				}
				else if(ftype.equals(Boolean.class)){
					f.set(output, rs.getByte(name) != 0);
				}
				else if(ftype.equals(Byte.class)) {
					f.set(output, rs.getByte(name));
				}
				else if(ftype.equals(Short.class)){
					f.set(output, (short)rs.getInt(name));
				}
				else if(ftype.equals(Integer.class)) {
					f.set(output, rs.getInt(name));
				}
				else if(ftype.equals(Long.class)) {
					f.set(output, rs.getLong(name));
				}
				else if(ftype.equals(Float.class)){
					f.set(output, rs.getFloat(name));
				}
				else if(ftype.equals(Double.class)){
					f.set(output, rs.getDouble(name));
				}

				else if(ftype.equals(String.class)) {
					f.set(output, rs.getString(name));
				}
				else if(ftype.equals(byte[].class)) {
					f.set(output, rs.getBytes(name));
				}
				else if(ftype.isInstance(List.class)) {
					String b64 = rs.getString(name);
					if(b64 == null)f.set(output, null);
					else{
						byte[] bytes = B64.decode(b64);
						ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
						List l = (List)mg.bytesToObject(stream);
						f.set(output, l);
					}
				}
				else {
					String b64 = rs.getString(name);
					if(b64 == null)f.set(output, null);
					else{
						byte[] bytes = B64.decode(b64);
						ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
						Object o = (Object)mg.bytesToObject(stream);
						f.set(output, o);
					}
				}
			}
			return (T)output;

		}
		
		/**
		*Deletes an object from a database given the primary key id.
		* @param id the primary key id of the object.
		*/
		
		public void delete(int id) {
			if(!Database.this.isopen)Database.this.open();
			String query = String.format("DELETE FROM %s WHERE dbid=%d;", name, id);
			if(ds!=null)ds.println(query);
			try(Statement stmt = c.createStatement()){
				 stmt.executeUpdate(query);
			}catch(SQLException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
		}
		/**
		* Deletes all the obejcts that meet a certain condition.
		* The delete query string is constructed as: 
		* DELETE * FROM [table name] WHERE [condition string].
		* @param condition The SQL query string that describes the delete condidtion.
		*/
		public void delete(String condition) {
			if(!Database.this.isopen)Database.this.open();
			String query = String.format("DELETE FROM %s WHERE %s;", name, condition);
			if(ds!=null)ds.println(query);
			try(Statement stmt = c.createStatement()){
				 stmt.executeUpdate(query);
			}catch(SQLException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
		}
				
	}

}
