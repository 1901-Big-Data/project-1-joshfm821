package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class gsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	static volatile private ArrayList<String> headers = new ArrayList<String>();
	static volatile private String[] years = {"1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970",
			"1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986",
			"1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002",
			"2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016"};
	{		
		String input = "Country Name,Country Code,Indicator Name,Indicator Code,1960,1961,1962,1963,1964,1965,"
				+ "1966,1967,1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,1982,1983,"
				+ "1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,"
				+ "2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016";
		String[] lineOne = input.split(",");
		
		for(String header: lineOne) {
			headers.add(header);
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		//handle header line
		if(line.charAt(0) == ',') {
			return;
		}
		
		if (line.contains("SE.TER.HIAT.BA.FE")) {
			double baGraduate = 0;
			if (baGraduate <= 30)
			{
				//this is to display result of search
				//context.write(new Text, new IntWritable);
			}
		}
		
		
				
		String[] data = line.split(",");
		for (String year: years) {
			try {
				int yearLocationInCSV = headers.indexOf(year);
				if (yearLocationInCSV >= data.length) {
					return;
				}
				int time = Integer.parseInt(data[yearLocationInCSV]);
				if(time > 0) {
					context.write(new Text(year), new IntWritable(time));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				return;
			} catch (NumberFormatException e){
				context.getCounter("debug", "bad input").increment(1L);
			}
		}
		
		
	}

}
