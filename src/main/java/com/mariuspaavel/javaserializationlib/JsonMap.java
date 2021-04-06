package com.mariuspaavel.javaserializationlib;
import java.util.*;
import java.io.*;
class JsonMap{
	boolean d = false;
	PrintStream ds = System.out;	

	private char c = 0;
	private InputStreamReader is = null;
	private PrintStream ps = null;

	private char nextChar(){
		try{
			int code = is.read();

			if(code == -1)throw new RuntimeException("No JSON object could be read, InputStream ended unexpectedly.");
			return c = (char)code;
		}catch(IOException e){
			throw new RuntimeException(e.getMessage());
		}
	}
	private void skipWS(){
		while(Character.isWhitespace(c))nextChar();
	}

	private Map<String, Object> readMap() throws IOException{
		if(d)ds.println("Reading map");
		if(c != '{')throw new RuntimeException(String.format("Invalid map object start '%c'", c));
		nextChar();
		Map<String, Object> result = new HashMap<String, Object>();
		boolean expectNext = false;
		outer:while(true){
			skipWS();
			switch(c){
				case '}': {
					if(expectNext)throw new RuntimeException("Expected next object after ','");
					if(d)ds.println("Reached the end of map");
					break outer;
				} 
				case '\"': 
					String name = readString();
					while(true){
						nextChar();
						skipWS();
						if(c == ':')break;
						else throw new RuntimeException("Invalid json map: no ':' character between key and value");
					}
					nextChar();
					skipWS();
					Object o = null;
					try{
						o = readObject();
					}catch(UnknownObject e){
						throw new RuntimeException(String.format("Unknown object starting with character '%c'", c));
					}
					if(!(o instanceof Number))nextChar();	
					while(Character.isWhitespace(c))nextChar();
					if(c == ','){
						expectNext = true;
						nextChar();
					}
					else expectNext = false;
					result.put(name, o);
					skipWS();
					break;
				default: throw new RuntimeException(String.format("Unexpected character '%c'", c));
			}
		}
		return result;
	}
	private List<Object> readList() throws IOException{
		if(d)ds.println("Reading list");
		if(c != '[')throw new RuntimeException("Invalid list start position (must start with '[')");
		List<Object> list = new ArrayList<Object>();
		boolean expectNext = false;
		nextChar();
		skipWS();
		while(true){
			if(c == ']'){
				if(d)ds.println("Reached the end of list");
				if(expectNext){
					throw new RuntimeException("orphaned ',' in list");
				}
				return list;
			}
			Object o = null;
			expectNext = false;
			try{
				o = readObject();
			}catch(UnknownObject e){
				throw new RuntimeException(String.format("Unexpected character '%c' in json"));
			}	
			list.add(o);
			if(!(o instanceof Number))nextChar();
			skipWS();
			if(c == ',')expectNext = true;
			else if(c == ']'){
				if(d)ds.println("Reached the end of list");
				return list;
			}else throw new RuntimeException("Missing ',' in list");
			skipWS();
		}
	}
	private String readString() throws IOException{
		if(d)ds.println("Reading String");
		if(c != '\"')throw new RuntimeException("Invalid string start position (must start with '\"')");
		StringBuilder sb = new StringBuilder();
		while(true){
			nextChar();
			if(c == '\\'){
				nextChar();
				switch(c){
					case 'n': sb.append('\n'); break;
					case 'b': sb.append('\b'); break;
					case 'f': sb.append('\f'); break;
					case 'r': sb.append('\r'); break;
					case 't': sb.append('\t'); break;
					case '\"': sb.append('\"'); break;
					case '\\': sb.append('\\'); break; 
				}
			}
			else if(c == '\"'){
				if(d)ds.println("Reached the end of string");
				break;
			}
			else sb.append(c);
		}
		return sb.toString();
	}
	private Number readNumber() throws IOException{
		if(d)ds.println("Reading number");
		StringBuilder sb = new StringBuilder();
		do{
			sb.append(c);
		}while(numChars(nextChar()));

		String s = sb.toString().toLowerCase();
		if(s.contains(".") || s.contains("e")){
			return new Double(s);
		}
		else return new Long(s);
	}
	private static boolean numChars(char c){
		if(Character.isDigit(c) || c == '.' || c == 'e' || c == 'E')return true;	
		else return false;
	}
	private boolean readBoolean() throws IOException{
		if(d)ds.println("Reading boolean");
		final String trueSequence = "true";
		final String falseSequence = "false";
		c = Character.toLowerCase(c);
		String expected = null;
		if(c == 't'){
			expected = trueSequence;
		}
		else if(c == 'f'){
			expected = falseSequence;
		}
		else throw new RuntimeException("invalid boolean value in json");
		
		for(int i = 1; i < expected.length(); i++){
			nextChar();
			c = Character.toLowerCase(c);
			if(c != expected.charAt(i))throw new RuntimeException("invalid boolean value in json");
		}
		if(d)ds.println("Reached the end of boolean");
		return expected == trueSequence;
	}
	private Object readNull() throws IOException{
		if(d)ds.println("reading null");
		final String expectedSequence = "null";
		for(int i = 1; i < expectedSequence.length(); i++){
			nextChar();
			c = Character.toLowerCase(c);
			if(c != expectedSequence.charAt(i))throw new RuntimeException("invalid null value in json");
		}		
		if(d)ds.println("Reached the end of null value");
		return null;
	}	
	private static class UnknownObject extends Exception{
		
	}	

	private Object readObject()throws IOException, UnknownObject{
		if(d)ds.printf("Reading object starting with character '%c' (x%x)\n", c , c & 0xffff);	
		if(Character.isDigit(c))return readNumber();
			
		switch(c){
			case '{': return readMap();
			case '[': return readList();
			case '\"': return readString();
			case 't': return readBoolean();
			case 'f': return readBoolean();
			case 'n': return readNull();
			case '.': return readNumber();
			default: throw new UnknownObject();
		}
	
	}
	public Object readObject(InputStreamReader is)throws IOException{
		this.is = is;
		nextChar();
		skipWS();
		try{
			return readObject();
		}catch(UnknownObject e){
			return null;
		}
	}
	private void writeString(String s, boolean indent, int depth) throws IOException{
		if(d)ds.println("Writing string");
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('\"');
		for(int i = 0; i < s.length(); i++){
			char ch = s.charAt(i);
			switch(c){
				case '\b': ps.print("\\b"); break;
				case '\f': ps.print("\\f"); break;
				case '\n': ps.print("\\n"); break;
				case '\r': ps.print("\\r"); break;
				case '\t': ps.print("\\t"); break;
				case '\"': ps.print("\\\""); break;
				case '\\': ps.print("\\\\"); break;
				default: ps.print(ch);
			}
		}
		ps.print('\"');
	}
	private void writeMap(Map<String, Object> m, boolean indent, int depth) throws IOException{
		if(d)ds.println("Writing map");
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('{');
		ps.print('\n');
		for(Iterator<String> iter = m.keySet().iterator(); iter.hasNext();){
			String s = iter.next();
			if(m.get(s) == null)continue;
			writeString(s, true, depth+1);
			ps.print(':');
			ps.print(' ');
			Object o = m.get(s);
			writeObject(o, false, depth+1);
			if(iter.hasNext())ps.print(',');
			ps.print('\n');
		}
		for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('}');	
	}
	private void writeList(List<Object> l, boolean indent, int depth) throws IOException{
		if(d)ds.println("Writing list");
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('[');
		ps.print('\n');
		for(Iterator<Object> iter = l.iterator(); iter.hasNext();){
			Object o = null;
			while(o == null)o = iter.next();
			writeObject(o, true, depth+1);
			if(iter.hasNext())ps.print(',');
			ps.print('\n');
		}
		for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print(']');

	}
	private void writeBoolean(Boolean b, boolean indent, int depth){
		if(d)ds.println("Writing boolean");
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print(b.toString());
	}
	private void writeNumber(Number n, boolean indent, int depth){
		if(d)ds.println("Writing number");
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print(n.toString());
	}
	private void writeNull(boolean indent, int depth){
		if(d)ds.println("Writing null");
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print("null");
	}
	private void writeObject(Object o, boolean indent, int depth) throws IOException{
		if(d)ds.println("Writing object");
		if(o == null)writeNull(indent, depth);
		Class cl = o.getClass();
		if(o instanceof List)writeList((List)o, indent, depth);
		else if(o instanceof Map)writeMap((Map)o, indent, depth);
		else if(o instanceof Number)writeNumber((Number)o, indent, depth);
		else if(o instanceof Boolean)writeBoolean((Boolean)o, indent, depth);
		else writeString(o.toString(), indent, depth);
	}
	public void writeObject(Object o, PrintStream ps) throws IOException{
		this.ps = ps;
		writeObject(o, false, 0);
	}
}
