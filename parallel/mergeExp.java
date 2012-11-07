/**
 * 
 * @file		-	mergeExp.java
 * 
 * @purpose	-	Merges multiple rows together into one matrix
 * 
 * @author 		-	Himanshu Jindal, Piyush Kansal
 * 
 */

package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;

import java.io.*;

public class mergeExp {

	public void runTask(String[] args) throws Exception{
		mergeExp.main(args);
    }

	public static void main( String[] args ) throws IOException {
		String 			_TAB_			=	"\t";
		String 			_SLASH_			=	"/";
		String 			_ZERO_			=	"0 ";
		String			_NEW_LINE_		=	"\n";
	    int 			_SPLITS_		=	5;

		/**
		 * Validate i/p parameters
		 */
		if( args.length != 4 ) {
			System.out.println( "Usage: " + mergeExp.class + " <ref-genes-on-hdfs> <ip-dir-on-hdfs> <op-filename-on-local-fs> <number-of-experiments>" );
			System.exit( 1 );
		}

		Path ip = new Path( args[0] );
		Path ip2 = new Path( args[1] );
		Path op = new Path( args[2] );
		int expCnt = Integer.parseInt(args[3]);

		Configuration conf = new Configuration();
        FileSystem ipFs = FileSystem.get( conf );

        /**
         * Check if the i/p path exists
         */
        if( !ipFs.exists( ip ) ) {
        	System.out.println( "Path: " + ip + " does not exist" );
            System.exit( 1 );
        }
        
        /**
         * Check if the i/p path is a file
         */
        if( ipFs.getFileStatus( ip ).isDir() ) {
        	System.out.println( "First argument to " + mergeExp.class + " should be a file" );
        	System.out.println( "Usage: " + mergeExp.class + " <ref-genes-on-hdfs> <ip-dir-on-hdfs> <op-filename-on-local-fs>" );
            System.exit( 1 );
        }

        /**
         * Check if the i/p path exists
         */
        if( !ipFs.exists( ip2 ) ) {
        	System.out.println( "Path: " + ip2 + " does not exist" );
            System.exit( 1 );
        }
        
        /**
         * Check if the i/p path is a file
         */
        if( !ipFs.getFileStatus( ip2 ).isDir() ) {
        	System.out.println( "First argument to " + mergeExp.class + " should be a directory" );
        	System.out.println( "Usage: " + mergeExp.class + " <ref-genes-on-hdfs> <ip-dir-on-hdfs> <op-filename-on-local-fs>" );
            System.exit( 1 );
        }

        /**
         * Check if the o/p file already exists
         */
        LocalFileSystem opFs = FileSystem.getLocal( conf );
        if( opFs.exists( op ) ) {
            System.out.println( "File: " + op.getName()  + " already exists. Please choose a different file name" );
            System.exit( 1 );
        }

		BufferedReader refGenBR = new BufferedReader( new InputStreamReader( ipFs.open( ip ) ) );
		BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( opFs.create( op ) ) );
		while( true ) {
			String curLine = refGenBR.readLine();
			if( null == curLine ) {
				break;
			}

			String sub[] = curLine.split( _TAB_, _SPLITS_ );
			
			Path curPath = new Path( args[1] + _SLASH_ + sub[3] );
			if( ipFs.exists(curPath) ) {
				BufferedReader expMBR = new BufferedReader( new InputStreamReader( ipFs.open( curPath ) ) );
				bw.write( expMBR.readLine() + _NEW_LINE_ );
				expMBR.close();
			} else {
				String zeroBuf = "";
				for( int i = 0 ; i < expCnt ; i++ ) {
					 zeroBuf += _ZERO_;
				}
				
				bw.write( zeroBuf + _NEW_LINE_ );
			}
		}
		
		refGenBR.close();
		bw.close();
	}
}