package com.mariuspaavel.javaserializationlib;
import java.util.*;
import java.io.*;
public class JsonMap{
	private static Map<String, Object> readMap(InputStreamReader is) throws IOException{
		Map<String, Object> result = new HashMap<String, Object>();
		outer:while(true){
			char c = (char)is.read();
			switch(c){
				case '}': break outer; 
				case '\"': 
					String name = readString(is);
					Object o = readObject(is);
					result.put(name, o);
					break;
				default: continue;
			}
		}
		return result;
	}
	private static List<Object> readList(InputStreamReader is) throws IOException{
		List<Object> list = new ArrayList<Object>();
		while(true){
			Object o = readObject(is);
			list.add(readObject(is));
			if((is.read()) == ']')return list;
		}
	}
	private static String readString(InputStreamReader is) throws IOException{
		StringBuilder sb = new StringBuilder();
		while(true){
			char c = (char)is.read();
			if(c == '\"')break;
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
				default: continue;
			}
		}
	}
	private static void writeString(String s, PrintStream ps) throws IOException{
		ps.print('\"');
		ps.print(s);
		ps.print('\"');
	}
	private static void writeMap(Map<String, Object> m, PrintStream ps, int depth) throws IOException{
		ps.print('\n');
		for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('{');
		ps.print('\n');
		for(Iterator<String> iter = m.keySet().iterator(); iter.hasNext();){
			String s = iter.next();
			for(int i = 0; i < depth+1; i++)ps.print('\t');
			writeString(s, ps);
			ps.print(':');
			ps.print(' ');
			writeObject(m.get(s), ps, depth+1);
			if(iter.hasNext())ps.print(',');
			ps.print('\n');
		}
		for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('}');	
	}
	private static void writeList(List<Object> l, PrintStream ps, int depth) throws IOException{
		ps.print('\n');
		for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print('[');
		for(Object o : l){
			writeObject(o, ps, depth+1);
		}
		for(int i = 0; i < depth; i++)ps.print('\t');
		ps.print(']');

	}
	private static void writeObject(Object o, PrintStream ps, int depth) throws IOException{
		Class cl = o.getClass();
		if(o instanceof List)writeList((List)o, ps, depth);
		else if(o instanceof Map)writeMap((Map)o, ps, depth);
		else writeString(o.toString(), ps);
	}
	public static void writeObject(Object o, PrintStream ps) throws IOException{
		writeObject(o, ps, 0);
	}
}
