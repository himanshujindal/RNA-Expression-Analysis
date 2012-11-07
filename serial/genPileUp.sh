#!/bin/bash -xvf

# Run for all the experiments
for curFile in `find ./data/ -name "t*.1.fq" -type f`
do
	# Find the filename
	fileName=`echo $curFile | awk -F"/" '{print $3}' | awk -F"." '{print $1}'`

	# Gapped/Ungapped alignment
	bwa aln ecoli.fa ./data/$fileName.1.fq > ./data/alignments/$fileName.1.bwa.sai
	bwa aln ecoli.fa ./data/$fileName.2.fq > ./data/alignments/$fileName.2.bwa.sai

	# Generate paired-ended alignment
	bwa sampe ecoli.fa ./data/alignments/$fileName.1.bwa.sai ./data/alignments/$fileName.2.bwa.sai \
	./data/$fileName.1.fq ./data/$fileName.2.fq > ./data/alignments/$fileName.sam

	# Create sorted alignments
	samtools view -bhS data/alignments/$fileName.sam | samtools sort - data/pileup/$fileName

	# Generate pileup file
	samtools mpileup -f ecoli.fa -s data/pileup/$fileName.bam > data/pileup/$fileName.pileup.out
done
