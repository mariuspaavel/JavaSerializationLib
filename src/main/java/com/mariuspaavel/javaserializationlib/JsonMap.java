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
	public static Object readObject(InputStreamReader is)throws IOException{
		while(true){
			
			char c = (char)is.read();
			switch(c){
				case '{': return readMap(is);
				case '[': return readList(is);
				case '\"': return readString(is);
				case '}': return null;
				case ']': return null;
				default: continue;
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
	private static void writeObject(Object o, PrintStream ps, boolean indent, int depth) throws IOException{
		Class cl = o.getClass();
		if(o instanceof List)writeList((List)o, ps, indent, depth);
		else if(o instanceof Map)writeMap((Map)o, ps, indent, depth);
		else writeString(o.toString(), ps, indent, depth);
	}
	public static void writeObject(Object o, PrintStream ps) throws IOException{
		writeObject(o, ps, false, 0);
	}
}
