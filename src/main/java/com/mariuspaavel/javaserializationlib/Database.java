package com.mariuspaavel.javaserializationlib;

import java.io.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import com.mariuspaavel.javautilities.*;


public abstract class Database {
	private static Database instance;
		
	Connection c;
	
	private Map<String, Table> tables = new HashMap<String, Table>();
		
	void addTable(Table t){
		if(isopen)throw new RuntimeException("Cannot add table when database is already opened");
		
		tables.put(t.getName(), t);
	
	}
	
	private boolean isopen;
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
		System.out.println("Opening database " + getName());
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
	      System.out.println("Opened database " + getName() + " successfully");
	      return c;
	}
	private void close() {
		if(!isopen)return;
		try {
			c.close();
			System.out.println("Closing database " + getName());
			isopen = false;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private Manager mg;
	protected Database(Manager mg) {
		this.mg = mg;
	}
	
	public String getName() {
		return getClass().getName();
	}
	
	
	
	public class Table <T extends DBObject> {
		private String name;
		public String getName() {
			return name;
		}
		
		private Class dataType;
		private Manager m;
		
	
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
			TreeMap<String, Field> fc = m.getClassFields(dataType);
			
			sb.append("CREATE TABLE ");
			sb.append(name);
			sb.append(" (");
			sb.append("dbid INT PRIMARY KEY");
			
			Iterator<String> fi = fc.keySet().iterator();
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
			
			System.out.println(sql);
			
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
			System.out.println(query);
			try(Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(query);) {

				System.out.println("Getting max id");
				
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
		
		
		public void insert(DBObject o) {
			if(!Database.this.isopen)Database.this.open();
			
			StringBuilder sb = new StringBuilder();
			sb.append("INSERT INTO ");
			sb.append(name);
			sb.append(" (");
			
			TreeMap<String, Field> fmap = m.getClassFields(dataType);
			
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
						sb.append(f.getBoolean(o));
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
					else if(fclass.equals(Boolean.class) 
						|| fclass.equals(Byte.class) 
						|| fclass.equals(Short.class) 
						|| fclass.equals(Integer.class) 
						|| fclass.equals(Long.class)
						|| fclass.equals(Float.class)
						|| fclass.equals(Double.class)
						) {
						sb.append(f.get(o).toString());
					}
					else if(fclass.equals(String.class)) {
						sb.append(f.get(o));
					}
					else if(fclass.equals(byte[].class)) {
						sb.append("?");
						blobs.put(blobIndex, (byte[])f.get(o));
						blobIndex++;
					}
					else if(fclass.isInstance(List.class)) {
						ByteArrayOutputStream stream = new ByteArrayOutputStream();
						m.ObjectToBytes(f.get(o), stream);
						byte[] bytes = stream.toByteArray();
						sb.append(B64.encode(bytes));
					}
					else {
						ByteArrayOutputStream stream = new ByteArrayOutputStream();
						m.ObjectToBytes(f.get(o), stream);
						byte[] bytes = stream.toByteArray();
						sb.append(B64.encode(bytes));
					}
				}
				
				String sql = sb.toString();
	
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
			      
			            System.out.println("Uploaded data");
			            
					}
				}
			}catch(IllegalArgumentException | IllegalAccessException  | SQLException e) {
				e.printStackTrace();
				throw new DBException(e.getMessage());
			}
		}
		
		
		public void setField(int id, String fieldName, Object value)  {
			if(!Database.this.isopen)Database.this.open();
				
			TreeMap<String, Field> fields = m.getClassFields(dataType);
			
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
				
				sb.append(value.toString());
		
				if(fclass.isInstance(List.class)) {
					ByteArrayOutputStream stream = new ByteArrayOutputStream();
					m.ObjectToBytes(f.get(value), stream);
					byte[] bytes = stream.toByteArray();
					sb.append(B64.encode(bytes));
				}
				else if(fclass.equals(byte[].class)) {
					sb.append("?");
					blob = (byte[])f.get(value);
				}
				else if(value instanceof Number || value instanceof Boolean){
					sb.append(value.toString());
				}
				else {
					ByteArrayOutputStream stream = new ByteArrayOutputStream();
					m.ObjectToBytes(value, stream);
					byte[] bytes = stream.toByteArray();
					sb.append(B64.encode(bytes));
				}

				sb.append(" WHERE dbid=");
				sb.append(id);
				sb.append(";");
				
				String sql = sb.toString();
				
				System.out.println(sql);
				
				
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
		
		
		
		public T get(int dbid) {
			if(!Database.this.isopen)Database.this.open();
			String sql = String.format("SELECT * FROM %s WHERE dbid=%s;", name, dbid);
					
			try(Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(name)){
				if(!rs.next())return null;
				return readRS(rs);
			}catch(SQLException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException | IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
			
		}
		

		public ArrayList<T> query(String condition)  {
			if(!Database.this.isopen)Database.this.open();
			
			ArrayList<T> output = new ArrayList<T>();
			String query = String.format("SELECT * FROM %s WHERE %s;", name, condition);
			System.out.println(query);
			try(Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery( query);) {
				while(rs.next()) {
					output.add((T) readRS(rs));
				}
			}catch(SQLException | IOException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
				e.printStackTrace();
				throw new DBException(e.getMessage());
			}
			System.out.println("QUERY RESULT:");
			System.out.println("----------------------------------------------------");
			for(T t : output)System.out.println(t);
			System.out.println("----------------------------------------------------");
			return output;
		}
		
		
		public Object getField(int id, String fieldName) {
			try(Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(String.format("SELECT %s FROM %s WHERE dbid=%d", name, fieldName, id));) {
				if(!rs.next())return null;
				Class ftype = m.getClassFields(dataType).get(fieldName).getType();
				
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
					byte[] bytes = B64.decode(b64);
					ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
					List l = (List)m.bytesToObject(stream);
					return l;
				}
				else {
					String b64 = rs.getString(name);
					byte[] bytes = B64.decode(b64);
					ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
					Object o = (Object)m.bytesToObject(stream);
					return o;
				}

			}catch(SQLException | IllegalArgumentException | SecurityException e) {
				e.printStackTrace();
				throw new DBException(e.getMessage());
			}
		}
		
		
		private T readRS(ResultSet rs) throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException {
			TreeMap<String, Field> dTypeFields = m.getClassFields(dataType);
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
					byte[] bytes = B64.decode(b64);
					ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
					List l = (List)m.bytesToObject(stream);
					f.set(output, l);
				}
				else {
					String b64 = rs.getString(name);
					byte[] bytes = B64.decode(b64);
					ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
					Object o = (Object)m.bytesToObject(stream);
					f.set(output, o);
				}
			}
			return (T)output;

		}
		
		
		
		public void delete(int id) {
			if(!Database.this.isopen)Database.this.open();
			String query = String.format("DELETE FROM %s WHERE dbid=%d;", name, id);
			System.out.println(query);
			try(Statement stmt = c.createStatement()){
				 stmt.executeQuery(query);
			}catch(SQLException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
		}
		public void delete(String condition) {
			if(!Database.this.isopen)Database.this.open();
			String query = String.format("DELETE FROM %s WHERE %s;", name, condition);
			System.out.println(query);
			try(Statement stmt = c.createStatement()){
				 stmt.executeQuery(query);
			}catch(SQLException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
		}
		
		
	}

	
}
