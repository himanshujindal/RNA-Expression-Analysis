/**
 * 
 * @file		-	geaMap.java
 * 
 * @purpose	-	Analyze Gene Expressions
 * 
 * @author 		-	Himanshu Jindal, Piyush Kansal
 * 
 */

package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.io.ThreadedLineOperator;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.Functional.Operation;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Vector;

/**
 * 
 * @class	-	geaMap
 * @purpose	-	To define a class to analyze Gene Expressions
 *
 */
public class geaMap extends JnomicsMapper<Text, NullWritable, Text, IntWritable> {

    private static final JnomicsArgument 	refgenes_arg 	= new JnomicsArgument( "refGenes", "Reference Genes", true );
	private static final String 			_TAB_			=	"\t";
	private static final String 			_HYPHEN_		=	"-";
    private static final String 			_DOTS_			=	"..";
    private static final int 				_SPLITS_		=	5;

	private String 							refGenes;
    private Vector<RefGenes> 				myVector 		= new Vector<RefGenes>();
    
    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return IntWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{ refgenes_arg };
    }

    /*
     * Class for multi-keys mapping
     */
    public static class RefGenes {
    	long start;
    	long end;
    	long geneID;

    	public RefGenes( long s, long e, long id ) { 
    		start = s;
    		end = e;
    		geneID = id;
    	}
    	
    	public String toString() {
    		return start + " " + end + " " + geneID + "\n";
    	}
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		refGenes = conf.get( refgenes_arg.getName() );

		Path ip = new Path( refGenes );
		FileSystem ipFs = FileSystem.get( conf );
        if( !ipFs.exists( ip ) ) {
            System.err.println( "File: " + ip.getName()  + " does not exists." );
            System.exit( 1 );
        }
        
        int index = 0;
        BufferedReader br = new BufferedReader( new InputStreamReader( ipFs.open( ip ) ) );
        
        /*
         * Creates the multi-key map
         */
		while( true ) {
			String curLine = br.readLine();
			if( null == curLine ) {
				break;
			}

			String sub[] = curLine.split( _TAB_, _SPLITS_ );
			long s = Long.parseLong( sub[0].substring( 0, sub[0].indexOf( _DOTS_ ) ) );
			long e = Long.parseLong( sub[0].substring( sub[0].indexOf( _DOTS_ ) + _DOTS_.length(), sub[0].length() ) );
			
			RefGenes tempMap = new RefGenes( s, e, Long.parseLong(sub[3]) );
			myVector.add( index, tempMap );
			
			++index;
		}
    }

    /*
     * Searches for a key
     */
    private String getGeneID( long index ) {
    	int start = 0, end = myVector.size();
    	
    	while( start <= end ) {
    		int pos = (start + end)/2;
    		RefGenes temp = myVector.get( pos );
    		
    		if( ( index >= temp.start ) && ( index <= temp.end ) ) {
    			//return temp.geneID;
    			return temp.geneID + _HYPHEN_ + (temp.end - temp.start + 1); 
    		}
    		else if( index < temp.start ) {
    			end = pos - 1;
    		}
    		else if( index > temp.end ) {
    			start = pos + 1;
    		}
    	}
    	
    	return null;
    }
    
    @Override
    protected void map( Text key, NullWritable value, Context context ) throws IOException, InterruptedException {
    	String keyStr = key.toString();
		String sub[] = keyStr.split( _TAB_, _SPLITS_ );
		
		long index = Long.parseLong( sub[1] );
		int depth = Integer.parseInt( sub[3] );
		String genID = getGeneID( index );
		
		if( null != genID ) {
			String[] curName = ((FileSplit)context.getInputSplit()).getPath().getName().split( _HYPHEN_ );
			context.write( new Text( genID + _HYPHEN_ + curName[0] ), new IntWritable( depth ) );
		}
    }
}