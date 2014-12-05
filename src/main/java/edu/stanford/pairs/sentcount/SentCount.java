package edu.stanford.pairs.sentcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SentCount extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "SentCount");
	      job.setJarByClass(SentCount.class);
//	      job.setNumReduceTasks(2);
	      job.setMapOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(Sentence.class);
	      
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);
	      
	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new SentCount(), args);
	      
	      System.exit(res);
	}
	
	// (first 5 words) -> {sentence, firstFiveWords?, length, ID}
	// for each key: check first 5 words if their |length-their length|<=1
	// && ID is different && firstFiveWords? == their firstFiveWords
	public static class Sentence implements Writable {
		public String sentence;
		//public String[] sentence;
		public boolean firstFiveWords; //false = last 5 words
		public int length;
		public int ID;
		
		public Sentence(){
			this.sentence = null;
			this.firstFiveWords = false;
			this.length = 0;
			this.ID = -1;
		}
		
		public Sentence(String sentence, boolean firstFiveWords, int length, int ID){
			this.sentence = sentence;
			this.firstFiveWords = firstFiveWords;
			this.length = length;
			this.ID = ID;
		}
		
		public void set(String sentence, boolean firstFiveWords, int length, int ID){
			this.sentence = sentence;
			this.firstFiveWords = firstFiveWords;
			this.length = length;
			this.ID = ID;	
		}
		
		public void write(DataOutput out) throws IOException {
//			WritableUtils.writeCompressedStringArray(out, sentence);
			out.writeUTF(sentence);
			out.writeBoolean(firstFiveWords);
			out.writeInt(length);
			out.writeInt(ID);
		}
		
		public void readFields(DataInput in) throws IOException {
//			sentence = WritableUtils.readCompressedStringArray(in);
			sentence = in.readUTF();
			firstFiveWords = in.readBoolean();
			length = in.readInt();
			ID = in.readInt();
		}
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, Sentence> {
		private static Text ffw = new Text();
		private static Text lfw = new Text();
		private static Sentence snt = new Sentence();
		private static Text sntt = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] IDsent = value.toString().split(" ",2);
			String sentInString = IDsent[1];
			String[] sentence = sentInString.split(" ");
			int l = sentence.length;
			int num = Integer.parseInt(IDsent[0]);
			String firstFiveWords = "";
			for (int i = 0; i < 5; i++){
				firstFiveWords += sentence[i] + " ";
			}
			//Sentence (String[], bool, int, int)
			snt.set(sentInString,true,l,num);
			ffw.set(firstFiveWords);
			context.write(ffw, snt);
			String lastFiveWords = "";
			for (int i = l-5; i < l; i++){
				lastFiveWords += " " + sentence[i];
			}
			snt.set(sentInString,false,l,num);
			lfw.set(lastFiveWords);
			context.write(lfw, snt);
			
		}
	}
	
	public static class Reduce extends Reducer<Text, Sentence, Text, IntWritable> {
		//private final static IntWritable ONE = new IntWritable(1);
		private static HashMap<Boolean,Integer> unique;
		static {
			unique = new HashMap<Boolean, Integer>();
	        unique.put(true,0);
	        unique.put(false,0);
		}
		private static HashMap<Boolean,Integer> same;
		static {
			same = new HashMap<Boolean, Integer>();
	        same.put(true,0);
	        same.put(false,0);
		}
		private static Text txt = new Text("jizz");
		private static IntWritable cnt = new IntWritable(0);
		private static int totalunique = 0;
		private static int totalsame = 0;

		@Override
		public void reduce(Text key, Iterable<Sentence> values, Context context)
			throws IOException, InterruptedException {
			ArrayList<Sentence> bucket = new ArrayList<Sentence>(50);
			Sentence sentence1,sentence2;
			int id1,id2;
			boolean first1,first2;
			int l1,l2;
			String s1,s2;
			boolean nearLength, candidate, similar, notu, u;
			int uniqueTmp, notUniqueTmp;
			
			//shove bucket into arraylist
			
			for (Sentence s : values){
				Sentence tmp = new Sentence(s.sentence, s.firstFiveWords, s.length, s.ID);
				bucket.add(tmp);
			}
			
			int bucketsize = bucket.size();
			
			//for each pair of unique item in bucket, see if they are edit distance <=1.
			//if yes, put it in a global hashset
			for (int i = 0; i < bucketsize; i++){
				//init crap for one of the sentences
				sentence1 = bucket.get(i);
				id1 = sentence1.ID;
				first1 = sentence1.firstFiveWords;
				l1 = sentence1.length;
				s1 = sentence1.sentence;
			
				for (int j = i+1; j < bucketsize; j++) {
					//init crap for the other sentence
					sentence2 = bucket.get(j);
					id2 = sentence2.ID;
					first2 = sentence2.firstFiveWords;
					l2 = sentence2.length;
					s2 = sentence2.sentence;
					nearLength = (l2 - l1 == 0) | (l2 - l1 == -1) | (l2 - l1 == 1);
					

					candidate = nearLength && first1==first2 && id1!=id2;
//					boolean goodDist = checkEditDist(s1,s2,l1,l2);
					similar = checkFiveWords(s1,s2,first1,first2,l1,l2);
					
					//don't bother checking unless candidate/similar are true
					notu = (candidate & similar) && checkEditDist(s1,s2,l1,l2);
					u = (candidate & !similar) && checkEditDist(s1,s2,l1,l2);
					
//					uniqueTmp = unique.get(u) + 1;
//					notUniqueTmp = same.get(notu) + 1;
//					unique.put(u, uniqueTmp);
//					same.put(notu, notUniqueTmp);
					
					if (notu){
						totalsame++;
					} else if (u){
						totalunique++;
					}

//					int currCount = (same.get(true) >> 1) + unique.get(true);
					int currCount = (totalsame >> 1) + totalunique;

					if ( (currCount % 1000000 == 0) || currCount > 429000000 ){
						cnt.set(currCount);
						txt.set("");
						context.write(txt, cnt);
					}

//					if (){
//						System.out.println(s1);
//						System.out.println(s2);
//						System.out.println("---");
//						if(checkEditDist(s1,s2)){
//							hs.add(p);
//							total++;
//							context.write(new Text(Integer.toString(id1) + " " + Integer.toString(id2)), new IntWritable(1));
//						}
					} //inner for
				} //outer for
			} //reduce


		//check if s1 and s2 differ by one word at most.
		public static boolean checkEditDist(String sp1, String sp2, int l1, int l2){
			String[] s1 = split(sp1,' ');
			String[] s2 = split(sp2,' ');
			
			int ptr1 = 0;
			int ptr2 = 0;
			int diff = 0;
			int l = s1.length > s2.length ? s1.length : s2.length;
			boolean strict = s1.length!=s2.length;
			boolean t1shorter = s1.length < s2.length;
			for (int i = 0; i<l; i++){
				if (ptr1 == s1.length && ptr2 == s2.length-1 || 
					ptr1 == s1.length-1 && ptr2 == s2.length) {
					diff++;
					continue;
				} //length delta 1 at least
				if (s1[ptr1].equals(s2[ptr2])) {
					ptr1++;
					ptr2++;
				} else { //different words detected
					if (!strict){ //same length => substitution
						ptr1++;
						ptr2++;
					} else { //different length, a word was inserted in the longer sequence
						if (t1shorter) ptr2++;
						else ptr1++;
					}
					diff++; //regardless, a difference was detected
				}
				if (diff>1) return false;
			}
			
			if (diff>1) return false;
			return true;
		} //checkEditDist
		
		public static boolean checkFiveWords(String sp1, String sp2, boolean first1, boolean first2, int l1, int l2){
			String[] s1 = split(sp1,' ');
			String[] s2 = split(sp2,' ');
			boolean checkFirst = first1 == false && first2 == false;
			boolean checkLast = first1 && first2;
			boolean firstSame = true;
			boolean lastSame = true;
			for (int i = 0; i < 5; i++){
				firstSame &= s1[i].equals(s2[i]);
			}
			for (int i = 5; i>0; i--){
				lastSame &= s1[l1-i].equals(s2[l2-i]);
			}
			return (checkFirst & firstSame) | (checkLast & lastSame);
		}
		
		public static String[] split(final String s, final char delimeter) {    
	        int count = 1;    
	        for (int i = 0; i < s.length(); i++)
	            if (s.charAt(i) == delimeter)
	                count++;

	        String[] array = new String[count];    
	        int a = -1;
	        int b = 0;

	        for (int i = 0; i < count; i++) {    
	            while (b < s.length() && s.charAt(b) != delimeter)
	                b++;

	            array[i] = s.substring(a + 1, b);
	            a = b;
	            b++;
	        }    
	        return array;    
	    }
	} //Reduce
	
	
} //Tool
//public class WordCount extends Configured implements Tool {
//	   
//	   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
//	      private final static IntWritable ONE = new IntWritable(1);
//	      private Text word = new Text();
//
//	      @Override
//	      public void map(LongWritable key, Text value, Context context)
//	              throws IOException, InterruptedException {
//	    	  System.out.println(value.toString());
//	         for (String token: value.toString().split("\\s+")) {
//	        	if (token.length() > 0 && Character.isLetter(token.charAt(0))) {
//	        		word.set(token.substring(0,1).toLowerCase());
//	        		context.write(word, ONE);
//	        	}
//	         }
//	      }
//	   }
//
//	   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
//	      @Override
//	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
//	              throws IOException, InterruptedException {
//	         int sum = 0;
//	         for (IntWritable val : values) {
//	            sum += val.get();
//	         }
//	         context.write(key, new IntWritable(sum));
//	      }
//	   }
//	}