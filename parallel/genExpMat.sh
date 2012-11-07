# Main script for calculating gene expression
# Himanshu Jindal, Piyush Kansal

#/bin/bash -xvf

# Validate i/p arguments
if [ $# -ne 9 ]
then
	echo "Invalid number of arguments"
	echo "Usage: $0 <fasta-file> <ref-genes-file> <folder-containing-experiments> <number-of-experiments> <jnomics-jar> <bwa-bin> <bcf-bin> <sam-bin> <op-file-on-local-FS>"
	echo "NOTE: Experiment names should be in increasing order of number, starting from 1. For eg,"
	echo -e "1.1.fq\n1.2.fq"
	echo -e "2.1.fq\n2.2.fq"
	echo -e "3.1.fq\n3.2.fq"
	exit 1
fi

# Initialize i/p arguments
FST_FILE=$1
PTT_FILE=$2
EXP_PATH=$3
EXP_CONT=$4
JAR_PATH=$5
BWA_FILE=$6
BCF_PATH=$7
SAM_PATH=$8
EXP_MTRX=$9
HDFS_ROOT=gea

export PATH=$PATH:.
rm -f *.fa.*

i=1
while [ $i -le $EXP_CONT ]
do
	mv $EXP_PATH/t$i.1.fq $EXP_PATH/$i.1.fq
	mv $EXP_PATH/t$i.2.fq $EXP_PATH/$i.2.fq
	i=`expr $i + 1`
done

echo "Creating BWA Index ..."
bwa index $FST_FILE
if [ $? -ne 0 ]
then
	echo "Some error occured while creating BWA Index. Exiting ..."
	exit 1
fi

echo -e "\nCreating archieve 1 ..."
tar -czvf $EXP_PATH/archive.tar $FST_FILE* $BWA_FILE
if [ $? -ne 0 ]
then
	echo "Some error occured while creating archieve 1. Exiting ..."
	exit 1
fi

echo -e "\nCreating $EXP_CONT paired-ended-reads on HDFS ..."
hadoop dfs -rmr $HDFS_ROOT/pe/
hadoop dfs -mkdir $HDFS_ROOT/pe/
i=1
while [ $i -le $EXP_CONT ]
do
	hadoop jar $JAR_PATH loader-pe $EXP_PATH/$i.1.fq $EXP_PATH/$i.2.fq $HDFS_ROOT/pe/$i
	if [ $? -ne 0 ]
	then
		echo "Some error occured while creating paired-ended reads. Exiting ..."
		exit 1
	fi

	i=`expr $i + 1`
done

echo -e "\nCreating $EXP_CONT bwa-aligns on HDFS ..."
hadoop dfs -rmr $HDFS_ROOT/bwa/
hadoop dfs -mkdir $HDFS_ROOT/bwa/
i=1
while [ $i -le $EXP_CONT ]
do
	hadoop jar $JAR_PATH job -archives $EXP_PATH/archive.tar#ecoli -mapper bwa_map -in $HDFS_ROOT/pe/$i.pe -out $HDFS_ROOT/bwa/$i -bwa_index ecoli/ecoli.fa -bwa_binary ecoli/bwa
	if [ $? -ne 0 ]
	then
		echo "Some error occured while creating bwa-aligns. Exiting ..."
		exit 1
	fi

	i=`expr $i + 1`
done

echo -e "\nCreating Samtools Index ..."
samtools faidx $FST_FILE
if [ $? -ne 0 ]
then
	echo "Some error occured while creating Samtools Index. Exiting ..."
	exit 1
fi

echo -e "\nCreating archieve 2 ..."
tar -czvf $EXP_PATH/archive2.tgz $FST_FILE $FST_FILE.fai $BCF_PATH $SAM_PATH
if [ $? -ne 0 ]
then
	echo "Some error occured while creating archieve 2. Exiting ..."
	exit 1
fi

echo -e "\nCreating $EXP_CONT pileups on HDFS ..."
hadoop dfs -rmr $HDFS_ROOT/samtools/
hadoop dfs -mkdir $HDFS_ROOT/samtools/
i=1
while [ $i -le $EXP_CONT ]
do
	hadoop jar $JAR_PATH job -archives $EXP_PATH/archive2.tgz#ecoli2 -mapper samtools_map -reducer samtools_snp_reduce -in $HDFS_ROOT/bwa/$i -out $HDFS_ROOT/samtools/$i -samtools_bin ecoli2/samtools -reference_fa ecoli2/ecoli.fa

	if [ $? -ne 0 ]
	then
		echo "Some error occured while creating samtools-align. Exiting ..."
		exit 1
	fi

	i=`expr $i + 1`
done

echo -e "\nCreating expression matrix values for all the genes on HDFS ..."
hadoop dfs -rmr $HDFS_ROOT/samtools/comb/
hadoop dfs -mkdir $HDFS_ROOT/samtools/comb/
i=1
while [ $i -le $EXP_CONT ]
do
	cnt=`hadoop dfs -count gea/samtools/$i/p* | wc -l`
	j=0
	while [ $j -lt $cnt ]
	do
		hadoop dfs -mv $HDFS_ROOT/samtools/$i/`printf "part-r-%05d" $j` $HDFS_ROOT/samtools/comb/$i-$j
		j=`expr $j + 1`
	done

	i=`expr $i + 1`
done

ref=`echo $PTT_FILE | awk -F"/" '{print $NF}'`
hadoop dfs -rm $HDFS_ROOT/$ref
hadoop dfs -copyFromLocal $PTT_FILE $HDFS_ROOT
hadoop dfs -rmr $HDFS_ROOT/pileup/

hadoop jar $JAR_PATH job -in $HDFS_ROOT/samtools/comb -out $HDFS_ROOT/pileup/ -mapper geaMap -refGenes $HDFS_ROOT/$ref -reducer geaReduce -expCnt $EXP_CONT
if [ $? -ne 0 ]
then
	echo "Some error occured while creating pileups. Exiting ..."
	exit 1
fi

echo -e "\nCreating expression matrix on local file system ..."
hadoop jar $JAR_PATH merge-fetch-exp $HDFS_ROOT/$ref $HDFS_ROOT/pileup/ $EXP_MTRX $EXP_CONT

echo "Program finished successfully"
