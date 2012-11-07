/**
 * 
 * @file		-	geaReduce.java
 * 
 * @purpose		-	Analyze Gene Expressions
 * 
 * @author 		-	Himanshu Jindal, Piyush Kansal
 * 
 */

package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedLineOperator;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.Functional.Operation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.*;
import java.util.HashMap;

public class geaReduce extends JnomicsReducer<Text, IntWritable, Text, NullWritable> {

	private static final JnomicsArgument 	exp_cnt = new JnomicsArgument( "expCnt", "Experiment Count", true );
	private static final String 		_OP_DIR_	= "mapred.output.dir";
	private static final String 		_NEW_LINE_	= "\n";
	private static final String 		_HYPHEN_	=	"-";
	private static final String 		_SPACE_		=	" ";
	private String 			opDir;
	private FileSystem			opFs;
	private int 				prev = 0;
	private int 				expCnt;
	HashMap<Integer, Integer> 		hm = new HashMap<Integer, Integer>();

	@Override
	public Class getOutputKeyClass() {
		return Text.class;
	}

	@Override
	public Class getOutputValueClass() {
		return NullWritable.class;
	}

	@Override
	public JnomicsArgument[] getArgs() {
        	return new JnomicsArgument[]{ exp_cnt };
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		expCnt = Integer.parseInt( conf.get( exp_cnt.getName() ) );
		opDir = conf.get( _OP_DIR_ );
		opFs = FileSystem.get(conf);
	}

	@Override		
	protected void reduce( Text key, final Iterable<IntWritable> values, final Context context ) throws IOException, InterruptedException {

		++prev;
		String[] tempArr = key.toString().split(_HYPHEN_);
		
		while( values.iterator().hasNext() ) {
			IntWritable temp = values.iterator().next();
			Integer depthSum = hm.get(Integer.parseInt(tempArr[2]));
			
			if(depthSum == null) {
				depthSum = temp.get();
			}
			else {
				depthSum += temp.get();
			}
			
			hm.put(Integer.parseInt(tempArr[2]), depthSum);
		}
		
		hm.put(Integer.parseInt(tempArr[2]), hm.get(Integer.parseInt(tempArr[2]))/Integer.parseInt(tempArr[1]));
		
		if( prev < expCnt )
			return;

		Path file = new Path( opDir + "/" + tempArr[0] );
		BufferedWriter out = new BufferedWriter( new OutputStreamWriter( opFs.create( file ) ) );

		for(Integer i = 1; i <= hm.size() ; i++) {
			Integer depth = hm.get(i);
			if(depth!=null) {
				out.write( depth + _SPACE_ );
			}
			else {
				out.write( "0" + _SPACE_ );
			}
		}
		
		for(Integer i = 1; i <= hm.size() ; i++) {
			hm.remove(i);
		}

		out.write( _NEW_LINE_ );
		out.close();
		prev = 0;
	}
}
