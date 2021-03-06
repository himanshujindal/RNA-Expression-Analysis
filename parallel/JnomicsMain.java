package edu.cshl.schatz.jnomics.tools;


import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

public class JnomicsMain extends Configured implements Tool {

    public static final Map<String,Class<? extends JnomicsMapper>> mapperClasses =
            new HashMap<String, Class<? extends JnomicsMapper>>(){
                {
                    put("bowtie2_map", Bowtie2Map.class);
                    put("bwa_map", BWAMap.class);
                    put("samtools_map", SamtoolsMap.class);
                    put("kcounter_map", KCounterMap.class);
                    put("geaMap", geaMap.class);
                }
            };

    public static final Map<String,Class<? extends JnomicsReducer>> reducerClasses =
            new HashMap<String, Class<? extends JnomicsReducer>>(){
                {
                    put("samtools_reduce", SamtoolsReduce.class);
                    put("samtools_snp_reduce", SamtoolsSnpReduce.class);
                    put("kcounter_reduce", KCounterReduce.class);
                    put("geaReduce", geaReduce.class);
                }
            };

    public static final Map<String,Class> helperClasses =
            new HashMap<String, Class>(){
                {
                    put("loader_pairend", PairedEndLoader.class);
                }
            };


    public static void printMainMenu(){
        System.out.println("Options:");
        System.out.println("");
        System.out.println("mapper-list\t:\tList available mappers");
        System.out.println("reducer-list\t:\tList available reducers");
        System.out.println("describe\t:\tDescribe a mapper or reducer");
        //System.out.println("helper-task-list\t:\tList all helper tasks");
        //System.out.println("helper-task\t:\tRun helper task");
		System.out.println("loader-pe\t:\tLoad paired end sequencing file into hdfs");
		System.out.println("merge-fetch-exp\t:\tMerge expression matrix on HDFS and fetch it to local file system");
		System.out.println("job\t\t:\tsubmit a job");
    }

    public static void main(String []args) throws Exception {
        if(args.length <1){
            printMainMenu();
            System.exit(-1);
        }

        if(args[0].compareTo("mapper-list") == 0){
            System.out.println("Available Mappers:");
            for(Object t : mapperClasses.keySet()){
                System.out.println(t);
            }
        }else if(args[0].compareTo("reducer-list") ==0){
            System.out.println("Available Reducers:");
            for(Object t : reducerClasses.keySet()){
                System.out.println(t);
            }
        }else if(args[0].compareTo("loader-pe") == 0){
        	PairedEndLoader.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].compareTo("merge-fetch-exp") == 0){
        	mergeExp.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].compareTo("helper-task-list") == 0){
            System.out.println("Available Helper Tasks:");
            for(Object t: helperClasses.keySet()){
                System.out.println(t);
            }
        }else if(args[0].compareTo("helper-task") == 0){
            if(args.length < 2){
                System.out.println("run helper-task-list to see available helper tasks");
            }else{
                Class exec = helperClasses.get(args[1]);
                if(exec == null){
                    System.out.println("Error: unknown helper " + args[1]);
                }else{
                   System.out.println("TODO: Implement runner");
                }
            }
        }else if(args[0].compareTo("describe") ==0){
            boolean found = false;
            if(args.length < 2){
                System.out.println("describe <mapper/reducer>");
            }
            for(String t: mapperClasses.keySet()){
                if(args[1].compareTo(t) == 0){
                    JnomicsArgument.printUsage(args[1] +" Arguments:", mapperClasses.get(t).newInstance().getArgs(),
                            System.out);
                    found = true;
                }
            }
            for(String t: reducerClasses.keySet()){
                if(args[1].compareTo(t) == 0){
                    JnomicsArgument.printUsage(args[1] +" Arguments:", reducerClasses.get(t).newInstance().getArgs(),
                            System.out);
                    found = true;
                }
            }
            if(!found){
                System.out.println("Unknwon mapper/reducer" + args[1]);
            }
            
        }else if(args[0].compareTo("job") == 0){
            String[] newArgs = new String[args.length -1 ];
            System.arraycopy(args,1,newArgs,0,args.length-1);
            System.exit(ToolRunner.run(new Configuration(), new JnomicsMain(), newArgs));
        }else{
            printMainMenu();
        }
    }

    
    @Override
    public int run(String[] args) throws Exception {
        JnomicsArgument map_arg = new JnomicsArgument("mapper","map task", true);
        JnomicsArgument red_arg = new JnomicsArgument("reducer","reduce task", false);
        JnomicsArgument in_arg = new JnomicsArgument("in","Input path", true);
        JnomicsArgument out_arg = new JnomicsArgument("out","Output path", true);

        JnomicsArgument []jargs = new JnomicsArgument[]{map_arg,red_arg,in_arg,out_arg};

        try{
            JnomicsArgument.parse(jargs,args);
        }catch(MissingOptionException e){
            System.out.println("Error missing options:" + e.getMissingOptions());
            System.out.println();
            ToolRunner.printGenericCommandUsage(System.out);
            JnomicsArgument.printUsage("Map-Reduce Options:",jargs,System.out);
            return 1;
        }

        Class<? extends JnomicsMapper> mapperClass = mapperClasses.get(map_arg.getValue());
        Class<? extends JnomicsReducer> reducerClass = red_arg.getValue() == null ? null : reducerClasses.get(red_arg.getValue());

        if(mapperClass == null){
            System.out.println("Bad Mapper");
            ToolRunner.printGenericCommandUsage(System.out);
            JnomicsArgument.printUsage("Map-Reduce Options:",jargs,System.out);
            return 1;
        }
        
        Configuration conf= getConf();

        /** get more cli params **/
        JnomicsMapper mapInst = ReflectionUtils.newInstance(mapperClass,conf);
        try{
            JnomicsArgument.parse(mapInst.getArgs(),args);
        }catch(MissingOptionException e){
            System.out.println("Error missing options:" + e.getMissingOptions());
            ToolRunner.printGenericCommandUsage(System.out);
            JnomicsArgument.printUsage("Map Options:",mapInst.getArgs(),System.out);
            return 1;
        }
        //add all aguments to configuration
        for(JnomicsArgument jarg: mapInst.getArgs()){
            if(jarg.getValue() != null)
                conf.set(jarg.getName(), jarg.getValue());
        }

        /** get more cli params **/
        JnomicsReducer reduceInst = null;
        if(reducerClass != null)
            reduceInst = ReflectionUtils.newInstance(reducerClass,conf);
        if( reduceInst != null){
            try{
                JnomicsArgument.parse(reduceInst.getArgs(),args);
            }catch(MissingOptionException e){
                System.out.println("Error missing options:" + e.getMissingOptions());
                ToolRunner.printGenericCommandUsage(System.out);
                JnomicsArgument.printUsage("Reduce Options:",reduceInst.getArgs(),System.out);
                return 1;
            }
            //add all arguments to configuration
            for(JnomicsArgument jarg: reduceInst.getArgs()){
                System.out.println(jarg.getName() +":"+jarg.getValue());
                if(jarg.getValue() != null)
                    conf.set(jarg.getName(), jarg.getValue());
            }
        }

        /** Build the Job **/

        DistributedCache.createSymlink(conf);

        Job job = new Job(conf);

        job.setMapperClass(mapperClass);
        job.setMapOutputKeyClass(mapInst.getOutputKeyClass());
        job.setMapOutputValueClass(mapInst.getOutputValueClass());
        job.setOutputKeyClass(mapInst.getOutputKeyClass());
        job.setOutputValueClass(mapInst.getOutputValueClass());

        if(mapInst.getCombinerClass() != null)
            job.setCombinerClass(mapInst.getCombinerClass());
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.setInputPaths(job,in_arg.getValue());
        SequenceFileOutputFormat.setOutputPath(job,new Path(out_arg.getValue()));
        
        if(reduceInst != null){
            if(reduceInst.getGrouperClass() != null)
                job.setGroupingComparatorClass(reduceInst.getGrouperClass());
            if(reduceInst.getPartitionerClass() != null)
                job.setPartitionerClass(reduceInst.getPartitionerClass());
            job.setReducerClass(reducerClass);
            job.setOutputKeyClass(reduceInst.getOutputKeyClass());
            job.setOutputValueClass(reduceInst.getOutputValueClass());
        }else{
            job.setNumReduceTasks(0);
        }
        job.setJarByClass(JnomicsMain.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
