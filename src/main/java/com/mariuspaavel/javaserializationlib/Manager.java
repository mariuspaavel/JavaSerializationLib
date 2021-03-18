package com.mariuspaavel.javaserializationlib;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import java.util.*;
import java.io.*;


public class Manager {
	private HashMap<Class, TreeMap<String, Field>> classInfo = new HashMap<Class, TreeMap<String, Field>>();
	private TreeMap<String, Class> classesOrdered = new TreeMap<String, Class>();
	public void register(Class c) {
		if(registrationLocked)return;
		TreeMap<String, Field> classFields = new TreeMap<String, Field>();
		for(Field field  : c.getDeclaredFields())
		{
		    if (field.isAnnotationPresent(Serial.class))
		        {
		              classFields.put(field.getName(), field);
		        }
		}
		classInfo.put(c, classFields);
		classesOrdered.put(c.getName(), c);
	}
	
	private HashMap<Class, Integer> classId = new HashMap<Class, Integer>();
	private HashMap<Integer, Class> idClass = new HashMap<Integer, Class>();
	
	private boolean registrationLocked = false;
	public void lockRegistration() {
		if(registrationLocked)return;
		registrationLocked = true;
		
		classId.put(Byte.class, 1);
		idClass.put(1, Byte.class);
		
		classId.put(Integer.class, 2);
		idClass.put(2, Integer.class);
		
		classId.put(Long.class, 3);
		idClass.put(3, Long.class);
		
		classId.put(String.class, 4);
		idClass.put(4, String.class);
		
		classId.put(ArrayList.class, 5);
		idClass.put(5, ArrayList.class);
		
		int id = 6;
		for(String name : classesOrdered.keySet()) {
			classId.put(classesOrdered.get(name), id);
			idClass.put(id, classesOrdered.get(name));
		}
		
	}
	TreeMap<String, Field> getClassFields(Class c){
		return classInfo.get(c);
	}
	
	
	public void fieldToBytes(Field f, Object o, ByteArrayOutputStream stream)  {
		lockRegistration();
		Class c = f.getType();
		
		try {
			if(c.equals(byte.class)) {
				stream.write(new byte[] {f.getByte(o)});
			}
			else if(c.equals(int.class)) {
				stream.write(intToByteArray(f.getInt(o)));
			}
			else if(c.equals(long.class)) {
				stream.write(longToByteArray(f.getLong(o)));
			}
			else {
				ObjectToBytes(f.get(o), stream);
			}
		}catch(IOException | IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
			throw new SerializationException(e.getMessage());
		}
		
	}
	
	public void ObjectToBytes(Object o, ByteArrayOutputStream stream) {
		lockRegistration();
		Class c = o.getClass();
		
		try {
			stream.write(intToByteArray(classId.get(c)));
			
			if(c.equals(Byte.class)) {
				stream.write(new byte[] {(Byte)o});
			}
			else if(c.equals(Integer.class)) {
				stream.write(intToByteArray((Integer)o));
			}
			else if(c.equals(Long.class)) {
				stream.write(longToByteArray((Long)o));
			}
			else if(c.equals(String.class)) {
				byte[] strbytes = ((String)o).getBytes();
				stream.write(intToByteArray(strbytes.length));
				stream.write(strbytes);
			}
			else if(c.equals(byte[].class)) {
				byte[] bytes = (byte[])o;
				stream.write(intToByteArray(bytes.length));
				stream.write(bytes);
			}
			else if(c.isInstance(List.class)) {
				List l = (List)o;
				stream.write(intToByteArray(l.size()));
				for(Object elem : l) {
					ObjectToBytes(elem, stream);
				}
			}
			
			else  {
				TreeMap<String, Field> fields = classInfo.get(c);
				if(fields == null)throw new RuntimeException(String.format("Class %s isn't registrated", c.getName()));
				for(String fname: fields.keySet()) {
					Field f = fields.get(fname);
					fieldToBytes(f, o, stream);
				}
			}
		}catch( IllegalArgumentException | IOException e) {
			e.printStackTrace();
			throw new SerializationException(e.getMessage());
		}
	}
	
	
	public Object bytesToObject(ByteArrayInputStream stream) {
		lockRegistration();
		
		byte[] buf = new byte[4];
		try {
			stream.read(buf);
			int serialid = byteArrayToInt(buf);
			Class c = idClass.get(serialid);
			if(c == null)throw new RuntimeException(String.format("Class %s hasn't been registrated", c.getName()));
		
			if(c.equals(Byte.class)) {
				buf = new byte[1];
				stream.read(buf);
				return buf[0];
			}
			else if(c.equals(Integer.class)) {
				buf = new byte[4];
				stream.read(buf);
				return byteArrayToInt(buf);
			}
			else if(c.equals(Long.class)) {
				buf = new byte[8];
				stream.read(buf);
				return byteArrayToLong(buf);
			}
			else if(c.equals(String.class)) {
				buf = new byte[4];
				stream.read(buf);
				int length = byteArrayToInt(buf);
				buf = new byte[length];
				stream.read(buf);
				return new String(buf);
			}
			else if(c.equals(byte[].class)) {
				buf = new byte[4];
				stream.read(buf);
				int length = byteArrayToInt(buf);
				buf = new byte[length];
				stream.read(buf);
				return buf;
			}
			else if(c.isInstance(List.class)) {
				buf = new byte[4];
				stream.read(buf);
				int length = byteArrayToInt(buf);
				List l = (List)c.getDeclaredConstructor().newInstance();
				for(int i = 0; i < length; i++) {
					l.add(bytesToObject(stream));
				}
				return l;
			}
			else {
				Object o = c.getDeclaredConstructor().newInstance();
				TreeMap<String, Field> cinf = classInfo.get(c);
				
				for(Field f : cinf.values()) {
					if(f.getType().equals(byte.class)) {
						buf = new byte[1];
						stream.read(buf);
						f.setByte(o, buf[0]);
					}
					else if(f.getType().equals(int.class)) {
						buf = new byte[4];
						stream.read(buf);
						f.setInt(o, byteArrayToInt(buf));
					}
					else if(f.getType().equals(long.class)) {
						buf = new byte[8];
						stream.read(buf);
						f.setLong(o, byteArrayToLong(buf));
					}
					else {
						bytesToObject(stream);
					}
				}
				return o;
			} 
		}catch( IOException | InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
			throw new SerializationException(e.getMessage());
		}
	}
	
	
	
	
	static byte[] intToByteArray(int value) {
	    return new byte[] {
	            (byte)(value >>> 24),
	            (byte)(value >>> 16),
	            (byte)(value >>> 8),
	            (byte)value};
	}
	
	static int byteArrayToInt(byte[] bytes) {
		return ((bytes[0] & 0xFF) << 24) |
				((bytes[1] & 0xFF) << 16) |
				((bytes[2] & 0xFF) << 8 ) |
				((bytes[3] & 0xFF) << 0 );
	}
	
	static byte[] longToByteArray(long value) {
	    return new byte[] {
	    		(byte)(value >>> 56),
	    		(byte)(value >>> 48),
	    		(byte)(value >>> 40),
	    		(byte)(value >>> 32),
	            (byte)(value >>> 24),
	            (byte)(value >>> 16),
	            (byte)(value >>> 8),
	            (byte)value};
	}
	
	static long byteArrayToLong(byte[] bytes) {
		return ((bytes[0] & 0xFFl) << 56) |
				((bytes[1] & 0xFFl) << 48) |
				((bytes[2] & 0xFFl) << 40) |
				((bytes[3] & 0xFFl) << 32) |
				((bytes[4] & 0xFFl) << 24) |
				((bytes[5] & 0xFFl) << 16) |
				((bytes[6] & 0xFFl) << 8 ) |
				((bytes[7] & 0xFFl) << 0 );
	}

	public void writeJson(Object o, PrintStream ps) throws IOException{
		lockRegistration();
		Object flatObject = flattenObject(o);
		System.out.println("Writing json");
		JsonMap.writeObject(flatObject, ps);
	}
	public Object readJson(InputStreamReader is) throws IOException{
		lockRegistration();
		Object flatObject = JsonMap.readObject(is);
		Object o = inflateObject(flatObject);
		return o;
	}


	private Map<String, Object> flattenClass(Object o){
		System.out.println("Flattening class");
		Map<String, Object> map = new HashMap<String, Object>();
		Class cl = o.getClass();
		TreeMap<String, Field> fields = getClassFields(cl);
		if(fields == null)throw new IllegalArgumentException("Unknown class");
		
		map.put("className", cl.getName());

		try{
			for(String s: fields.keySet()){
				Field f = fields.get(s);
				Class fieldType = f.getType();
				if(fieldType.equals(byte.class))map.put(s, Byte.toString(f.getByte(o)));
				if(fieldType.equals(int.class))map.put(s, Byte.toString(f.getByte(o)));
				if(fieldType.equals(long.class))map.put(s, Byte.toString(f.getByte(o)));
				map.put(s, flattenObject(f.get(o)));
			}
		}catch(IllegalAccessException e){
			e.printStackTrace();
			throw new RuntimeException("Class flattening failed");
		}
		return map;
	}	

	private List<Object> flattenList(List<Object> inputList){
		List<Object> outputList = new ArrayList<Object>();
		for(Object inputObject : inputList){
			outputList.add(flattenObject(inputObject));
		}
		return outputList;

	}
	private Object flattenObject(Object inputObject){
		if(inputObject instanceof String)return inputObject;	
		else if(inputObject instanceof List)return flattenList((List)inputObject);		
		else if(classId.containsKey(inputObject.getClass()))return flattenClass(inputObject);
		else return inputObject.toString();
	}

	private Object inflateClass(Map<String, Object> input){
		String className = (String)input.get("className");
		Class cl = classesOrdered.get(className);
		Object output = null;
		try{
			output = cl.getDeclaredConstructor().newInstance();
		}catch(NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e){
			e.printStackTrace();
			throw new RuntimeException("Failed to inflate class");
		}
		TreeMap<String, Field> cinf = classInfo.get(cl);
		
		try{
			for(String fieldName : cinf.keySet()){
				Object inputObject = input.get(fieldName);
				if(inputObject == null)continue;
				Field field = cinf.get(fieldName);
				Class fieldType = field.getType();

				if(fieldType.equals(byte.class))field.setByte(output, ((Byte)input.get(fieldName)).byteValue());
				else if(fieldType.equals(int.class))field.setInt(output, ((Integer)input.get(fieldName)).intValue());
				else if(fieldType.equals(long.class))field.setLong(output, ((Long)input.get(fieldName)).longValue());
				else{
					Object inflatedObject = inflateObject(inputObject);
					field.set(output, inflatedObject);
				}
			}
		}catch(IllegalAccessException e){
			e.printStackTrace();
			throw new RuntimeException("Failed to inflate class");
		}
		return output;
	}
	private List<Object> inflateList(List<Object> inputList){
		List<Object> output = new ArrayList<Object>();
		for(Object o : inputList){
			output.add(inflateObject(o));
		}
		return output;
	}
	private Object inflateObject(Object input){
		if(input instanceof Map)return inflateClass((Map<String, Object>)input);
		else if(input instanceof List)return inflateList((List)input);
		else return input; 
	}
	
}
