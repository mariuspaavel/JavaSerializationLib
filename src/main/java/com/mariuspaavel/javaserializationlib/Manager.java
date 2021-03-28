package com.mariuspaavel.javaserializationlib;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import java.util.*;
import java.io.*;

import com.mariuspaavel.javautilities.*;


/**
* A class that handles the serialization and deserialization of objects. 
* The fields of a class that is to be serialized/deserialized by this class must be marked with the @S annotation.
* The Manager class has two states: the init state and the working state. During the init state, all classes that this class
* will serialize will have to be passed to the register method. The working state begins when first object is serialized or
* deserialized using this class. After that, no more classes can be registrated. This manager generates an id for every class 
* that is registrated. The id based on the alphabetical order of classes registrated. In order to have working results when 
* transmitting an object from one machine to another, the exact same set of classes must be registrated on both machines.
*/

public class Manager {
	
	private HashMap<Class, TreeMap<String, Field>> classInfo = new HashMap<Class, TreeMap<String, Field>>();
	private TreeMap<String, Class> classesOrdered = new TreeMap<String, Class>();
	
	/**
	*Register a class during th init phase.
	*A class cannot be serialized or deserialized without registrating it.
	*/

	public void register(Class c) {
		if(registrationLocked)throw new RuntimeException("Classes can only be registrated on the init phase");
		TreeMap<String, Field> classFields = new TreeMap<String, Field>();
		for(Field field  : c.getDeclaredFields())
		{
			if (field.isAnnotationPresent(S.class))
			{
		    		classFields.put(field.getName(), field);
			}
		}
		classInfo.put(c, classFields);
		classesOrdered.put(c.getName(), c);
		if(d)ds.println(String.format("Registrated class %s", c.getName()));
	}
	
	private boolean d = false;
	PrintStream ds = System.out;

	/**
	*Turn on debug printing.
	*/
	public void debugOn(){
		d = true;
	}
	/**
	*Turn off debug printing.
	*/
	public void debugOff(){
		d = false;
	}
	/**
	*Change where debug is printed. Default value is System.out
	*@param stream The PrintStream where debug is printed.
	*/
	public void setDebugPrintStream(PrintStream stream){
		ds = stream;	
	}
	

	private HashMap<Class, Integer> classId = new HashMap<Class, Integer>();
	private HashMap<Integer, Class> idClass = new HashMap<Integer, Class>();

	

	
	private boolean registrationLocked = false;
	
	/**
	*End the initialization phase of the manager.
	* After that no more classes can be registrated. This method triggers automatically when an object is passed to the serialization or deserialization methods.
	*/

	public void lockInitialization() {
		if(registrationLocked)return;
		registrationLocked = true;
		
		classId.put(Boolean.class, 1);
		idClass.put(1, Boolean.class);
		
		classId.put(Byte.class, 2);
		idClass.put(2, Byte.class);
			
		classId.put(Short.class, 3);
		idClass.put(3, Short.class);
	
		classId.put(Integer.class, 4);
		idClass.put(4, Integer.class);
		
		classId.put(Long.class, 5);
		idClass.put(5, Long.class);

		classId.put(Float.class, 6);
		idClass.put(6, Float.class);
			
		classId.put(Double.class, 7);
		idClass.put(7, Double.class);	

		classId.put(String.class, 8);
		idClass.put(8, String.class);
		
		classId.put(ArrayList.class, 9);
		idClass.put(9, ArrayList.class);
		
		int id = 10;
		for(String name : classesOrdered.keySet()) {
			classId.put(classesOrdered.get(name), id);
			idClass.put(id, classesOrdered.get(name));
		}
		if(d)ds.println("Registration locked");
		
	}
	TreeMap<String, Field> getClassFields(Class c){
		return classInfo.get(c);
	}
	
	
	private void fieldToBytes(Field f, Object o, OutputStream stream)  {
		lockInitialization();
		Class c = f.getType();
		
		if(d)ds.println(String.format("Serializing field \"%s\" type \"%s\"", f.getName(), c.getName()));

		try {
			if(c.equals(boolean.class)){
				NumberSerializer.writeBoolean(f.getBoolean(o), stream);
			}
			else if(c.equals(byte.class)) {
				NumberSerializer.writeByte(f.getByte(o), stream);
			}
			else if(c.equals(short.class)){
				NumberSerializer.writeShort(f.getShort(o), stream);
			}
			else if(c.equals(int.class)) {
				NumberSerializer.writeInt(f.getInt(o), stream);
			}
			else if(c.equals(long.class)) {
				NumberSerializer.writeLong(f.getLong(o), stream);
			}
			else if(c.equals(float.class)){
				NumberSerializer.writeFloat(f.getFloat(o), stream);
			}
			else if(c.equals(double.class)){
				NumberSerializer.writeDouble(f.getDouble(o), stream);
			}
			
			else {
				ObjectToBytes(f.get(o), stream);
			}
		}catch(IOException | IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
			throw new SerializationException(e.getMessage());
		}
		
	}
		
	/**
	*Write an object to an output stream as octets.
	* In order for this method to work, the class must be registrated and have to be serialized fields marked with the @S annotation
	* @param o The object that is to be serialized.
	* @param stream The OutputStream where the object is written.
	*/

	public void ObjectToBytes(Object o, OutputStream stream) {
		lockInitialization();
		
		try {
			if(o == null){
				stream.write(0);
				return;
			}
			Class c = o.getClass();
			
			if(d)ds.println(String.format("Serializing class %s", c.getName()));
		
			Integer inputClassId = classId.get(c);
			if(inputClassId == null)throw new RuntimeException(String.format("Class %s hasn't been registrated.", c.getName()));	
			stream.write(NumberSerializer.intToByteArray(classId.get(c)));
			
			if(c.equals(Byte.class)) {
				stream.write(new byte[] {(Byte)o});
			}
			else if(c.equals(Integer.class)) {
				stream.write(NumberSerializer.intToByteArray((Integer)o));
			}
			else if(c.equals(Long.class)) {
				stream.write(NumberSerializer.longToByteArray((Long)o));
			}
			else if(c.equals(String.class)) {
				byte[] strbytes = ((String)o).getBytes();
				stream.write(NumberSerializer.intToByteArray(strbytes.length));
				stream.write(strbytes);
			}
			else if(c.equals(byte[].class)) {
				byte[] bytes = (byte[])o;
				stream.write(NumberSerializer.intToByteArray(bytes.length));
				stream.write(bytes);
			}
			else if(c.isInstance(List.class)) {
				List l = (List)o;
				stream.write(NumberSerializer.intToByteArray(l.size()));
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
	
	/**
	*Deserializes an octet stream to an object.
	* In order for this to work, to be serialized fields must be marked by the @S annotation and the object must be registrated.
	* @param stream The stream from which the object is read.
	* @return The deserialized object.
	*/
	
	public Object bytesToObject(InputStream stream) {
		lockInitialization();
		
		byte[] buf = new byte[4];
		try {
			stream.read(buf);
			int serialid = NumberSerializer.byteArrayToInt(buf);
			if(serialid == 0)return null;
			Class c = idClass.get(serialid);
			if(c == null)throw new RuntimeException(String.format("No class registrated with an id of %d", serialid));
			if(d)ds.println(String.format("Deserializing class %s", c.getName()));		

			if(c.equals(Boolean.class))return NumberSerializer.readBoolean(stream);
			else if(c.equals(Byte.class))return NumberSerializer.readByte(stream);
			else if(c.equals(Short.class))return NumberSerializer.readShort(stream);
			else if(c.equals(Integer.class))return NumberSerializer.readInt(stream);
			else if(c.equals(Long.class))return NumberSerializer.readLong(stream);
			else if(c.equals(String.class)) {
				int length = NumberSerializer.readInt(stream);
				buf = new byte[length];
				stream.read(buf);
				return new String(buf);
			}
			else if(c.equals(byte[].class)) {
				int length = NumberSerializer.readInt(stream);
				buf = new byte[length];
				stream.read(buf);
				return buf;
			}
			else if(c.isInstance(List.class)) {
				int length = NumberSerializer.readInt(stream);
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
					if(f.getType().equals(boolean.class))f.setBoolean(o, NumberSerializer.readBoolean(stream));
					else if(f.getType().equals(byte.class))f.setByte(o, NumberSerializer.readByte(stream));
					else if(f.getType().equals(short.class))f.setShort(o, NumberSerializer.readShort(stream));
					else if(f.getType().equals(int.class))f.setInt(o, NumberSerializer.readInt(stream));
					else if(f.getType().equals(long.class))f.setLong(o, NumberSerializer.readLong(stream));
					else if(f.getType().equals(float.class))f.setFloat(o, NumberSerializer.readFloat(stream));
					else if(f.getType().equals(double.class))f.setDouble(o, NumberSerializer.readDouble(stream));
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

	private JsonMap jsonInstance;
	private JsonMap getJsonInstance(){
		if(jsonInstance != null)return jsonInstance;
		else{
			jsonInstance = new JsonMap();
			jsonInstance.d = d;
			jsonInstance.ds = ds;
			return jsonInstance;
		}
	}
	
	/**
	*Writes an object to an output stream as JSON. 
	* In order for this to work, to be serialized fields must be marked with the @S annotation and the class must be registrated.
	* @param o The object to be registrated.
	* @param os The stream where the JSON object is written.
	*/	
	
	public void writeJson(Object o, OutputStream os) throws IOException{
		lockInitialization();
		PrintStream ps = new PrintStream(os);
		Object flatObject = flattenObject(o);
		if(d)ds.println("Writing json");
		getJsonInstance().writeObject(flatObject, ps);
	}
	
	/**
	*Reads a JSON object from an input stream.
	* The class name must be included as a "className" value that is assigned the full name of the class including the package.
	* In order for this to work, to be serialized fields must be marked with the @S annotation and the class must be registrated.	
	*/
	public Object readJson(InputStream is) throws IOException{
		lockInitialization();
		if(d)ds.println("Reading json");
		InputStreamReader ir = new InputStreamReader(is);
		Object flatObject = getJsonInstance().readObject(ir);
		Object o = inflateObject(flatObject);
		return o;
	}


	private Map<String, Object> flattenClass(Object o){
		if(d)ds.println("Flattening class");
		Map<String, Object> map = new HashMap<String, Object>();
		Class cl = o.getClass();
		TreeMap<String, Field> fields = getClassFields(cl);
		if(fields == null)throw new IllegalArgumentException("Unknown class");
		
		map.put("className", cl.getName());

		try{
			for(String s: fields.keySet()){
				Field f = fields.get(s);
				Class fieldType = f.getType();

				if(fieldType.equals(boolean.class))map.put(s, f.getBoolean(o));
				else if(fieldType.equals(byte.class))map.put(s, new Long(f.getByte(o)));
				else if(fieldType.equals(short.class))map.put(s, new Long(f.getShort(o)));
				else if(fieldType.equals(int.class))map.put(s, new Long(f.getInt(o)));
				else if(fieldType.equals(long.class))map.put(s, f.getLong(o));
				else if(fieldType.equals(float.class))map.put(s, new Double(f.getFloat(o)));
				else if(fieldType.equals(double.class))map.put(s, f.getDouble(o));

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
	private Object flattenNumber(Object inputNumber){
		Class type = inputNumber.getClass();	

		if(inputNumber instanceof Byte){
			return new Long(((Byte)inputNumber).byteValue());
		}else if(inputNumber instanceof Short){
			return new Long(((Short)inputNumber).shortValue());
		}else if(inputNumber instanceof Integer){
			return new Long(((Integer)inputNumber).intValue());
		}else if(inputNumber instanceof Long){
			return inputNumber;
		}else if(inputNumber instanceof Float){
			return new Double(((Float)inputNumber).floatValue());
		}else if(inputNumber instanceof Double){
			return inputNumber;
		}else if(inputNumber instanceof Boolean){
			return inputNumber;
		}
		else throw new RuntimeException(String.format("Error flattening number: %s is not a numeral type", inputNumber.getClass().toString()));
	}
	private Object flattenObject(Object inputObject){
		if(d)ds.println("flattening object");
		if(inputObject == null)return null;
		else if(inputObject instanceof String)return inputObject;	
		else if(inputObject instanceof List)return flattenList((List)inputObject);
		else if(inputObject instanceof Number)return flattenNumber((Number)inputObject);
		else if(classId.containsKey(inputObject.getClass()))return flattenClass(inputObject);
		else return inputObject.toString();
	}

	private Object inflateClass(Map<String, Object> input){
		if(d)ds.println("inflating class");
		String className = (String)input.get("className");
		if(d)ds.println(String.format("Class name: %s", className));
		Class cl = classesOrdered.get(className);
		if(cl == null)throw new RuntimeException(String.format("Class \"%s\" not registrated", className));
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
				if(fieldName.equals("className"))continue;
				Object inputObject = input.get(fieldName);
				if(inputObject == null)continue;
				Field field = cinf.get(fieldName);
				Class fieldType = field.getType();
				
				if(fieldType.equals(byte.class))field.setByte(output, (byte)((Long)inputObject).longValue());
				else if(fieldType.equals(short.class))field.setShort(output, (short)((Long)inputObject).longValue());
				else if(fieldType.equals(int.class))field.setInt(output, (int)((Long)inputObject).longValue());
				else if(fieldType.equals(long.class))field.setLong(output, ((Long)inputObject).longValue());
				else if(fieldType.equals(float.class))field.setFloat(output, (float)((Double)inputObject).doubleValue());
				else if(fieldType.equals(double.class))field.setDouble(output, ((Double)inputObject).doubleValue());
				else if(fieldType.equals(boolean.class))field.setBoolean(output, ((Boolean)inputObject).booleanValue());
				
				else{
					if(inputObject == null)field.set(output, null);
					else if(fieldType.equals(Byte.class))field.set(output, new Byte((byte)((Long)inputObject).longValue()));
					else if(fieldType.equals(Short.class))field.set(output, new Short((short)((Long)inputObject).longValue()));
					else if(fieldType.equals(Integer.class))field.set(output, new Integer((int)((Long)inputObject).longValue()));
					else if(fieldType.equals(Long.class))field.set(output, (Long)inputObject);
					else if(fieldType.equals(Float.class))field.set(output, new Float((float)((Double)inputObject).doubleValue()));
					else if(fieldType.equals(Double.class))field.set(output, new Double(((Double)inputObject).doubleValue()));
					else if(fieldType.equals(Boolean.class))field.set(output, (Boolean)inputObject);
			
					else{
						Object inflatedObject = inflateObject(inputObject);
						field.set(output, inflatedObject);
					}
				}
			}
		}catch(IllegalAccessException e){
			e.printStackTrace();
			throw new RuntimeException("Failed to inflate class");
		}
		if(d)ds.println("Inflating class finished");
		return output;
	}
	private List<Object> inflateList(List<Object> inputList){
		if(d)ds.println("Inflating list");
		List<Object> output = new ArrayList<Object>();
		for(Object o : inputList){
			output.add(inflateObject(o));
		}
		return output;
	}
	private Object inflateObject(Object input){
		if(d)ds.println("Inflating object");
		if(input instanceof Map)return inflateClass((Map<String, Object>)input);
		else if(input instanceof List)return inflateList((List)input);
		else return input; 
	}
	
}
