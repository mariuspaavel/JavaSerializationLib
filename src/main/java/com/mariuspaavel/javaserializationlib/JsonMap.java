package com.mariuspaavel.javaserializationlib;
import java.util.*;
import java.io.*;
public class JsonMap{
	private static Map<String, Object> readMap(InputStreamReader is) throws IOException{
		//System.out.println("Reading map");
		Map<String, Object> result = new HashMap<String, Object>();
		outer:while(true){
			char c = (char)is.read();
			switch(c){
				case '}': {
					//System.out.println("Reached the end of map");
					break outer;
				} 
				case '\"': 
					String name = readString(is);
					Object o = readObject(is);
					result.put(name, o);
					break;
				default: continue outer;
			}
		}
		return result;
	}
	private static List<Object> readList(InputStreamReader is) throws IOException{
		//System.out.println("Reading list");
		List<Object> list = new ArrayList<Object>();
		while(true){
			char c = (char)is.read();
			if(c == ']'){
				//System.out.println("Reached the end of list");
				return list;
			}
			Object o = readObject(is);
			if(o == null)break;
			else list.add(o);
		}
		return list;
	}
	private static String readString(InputStreamReader is) throws IOException{
		//System.out.println("Reading String");
		StringBuilder sb = new StringBuilder();
		while(true){
			char c = (char)is.read();
			if(c == '\"'){
				//System.out.println("Reached the end of string");
				break;
			}
			sb.append(c);
		}
		return sb.toString();
	}
	private static Number readNumber(InputStreamReader is, char firstDigit) throws IOException{
		StringBuilder sb = new StringBuilder();
		sb.append(firstDigit);
		char c = 0;
		while(numChars(c = (char)is.read())){
			sb.append(c);
		}
		String s = sb.toString().toLowerCase();
		if(s.contains(".") || s.contains("e")){
			return new Double(s);
		}
		else return new Long(s);
	}
	private static boolean numChars(char c){
		switch(c){
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
			case '.':
			case '-':
			case 'e':
			case 'E':
			return true;
			default: return false;
		}
	}
	private static boolean readBoolean(InputStreamReader is, char firstDigit) throws IOException{
		final String trueSequence = "true";
		final String falseSequence = "false";
		char c = Character.toLowerCase(firstDigit);
		String expected = null;
		if(c == 't'){
			expected = trueSequence;
		}
		else if(c == 'f'){
			expected = falseSequence;
		}
		else throw new RuntimeException("invalid boolean value in json");
		
		for(int i = 1; i < expected.length(); i++){
			c = (char)is.read();
			if(c != expected.charAt(i))throw new RuntimeException("invalid boolean value in json");
		}
		return expected == trueSequence;
	}
	public static Object readObject(InputStreamReader is)throws IOException{
		while(true){
			
			char c = (char)is.read();
			switch(c){
				case '{': return readMap(is);
				case '[': return readList(is);
				case '\"': return readString(is);
				case '}': return null;
				case ']': return null;
				case 't': return readBoolean(is, c);
				case 'f': return readBoolean(is, c);
				default: 
				if(numChars(c))return readNumber(is, c);
				else continue;
			}
		}
	}
	private static void writeString(String s, PrintStream ps, boolean indent, int depth) throws IOException{
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('\"');
		ps.print(s);
		ps.print('\"');
	}
	private static void writeMap(Map<String, Object> m, PrintStream ps, boolean indent, int depth) throws IOException{
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('{');
		ps.print('\n');
		for(Iterator<String> iter = m.keySet().iterator(); iter.hasNext();){
			String s = iter.next();
			writeString(s, ps, true, depth+1);
			ps.print(':');
			ps.print(' ');
			Object o = m.get(s);
			writeObject(o, ps, false, depth+1);
			if(iter.hasNext())ps.print(',');
			ps.print('\n');
		}
		for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('}');	
	}
	private static void writeList(List<Object> l, PrintStream ps, boolean indent, int depth) throws IOException{
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('[');
		ps.print('\n');
		for(Iterator<Object> iter = l.iterator(); iter.hasNext();){
			Object o = iter.next();
			writeObject(o, ps, true, depth+1);
			if(iter.hasNext())ps.print(',');
			ps.print('\n');
		}
		for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print(']');

	}
	private static void writeBoolean(Boolean b, PrintStream ps, boolean indent, int depth){
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print(b.toString());
	}
	private static void writeNumber(Number n, PrintStream ps, boolean indent, int depth){
		if(indent)for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print(n.toString());
	}
	private static void writeObject(Object o, PrintStream ps, boolean indent, int depth) throws IOException{
		Class cl = o.getClass();
		if(o instanceof List)writeList((List)o, ps, indent, depth);
		else if(o instanceof Map)writeMap((Map)o, ps, indent, depth);
		else if(o instanceof Number)writeNumber((Number)o, ps, indent, depth);
		else if(o instanceof Boolean)writeBoolean((Boolean)o, ps, indent, depth);
		else writeString(o.toString(), ps, indent, depth);
	}
	public static void writeObject(Object o, PrintStream ps) throws IOException{
		writeObject(o, ps, false, 0);
	}
}
