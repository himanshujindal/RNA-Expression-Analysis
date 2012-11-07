RNA-Expression-Analysis
=======================

- RNA expression analysis is algorithmically complex task

- Our system address this challenge by decreasing its execution time using Hadoop. It first creates a data pipeline to connect the various phases involved in expression analysis and then uses MapReduce to decrease the algorithmic complexity. We developed this functionality for Jnomics, an open souce genome sequence analysis framework

- Used MapReduce, Hadoop, Java, sequence aligners Bowtie2 and Bwa, sequence storage map Samtools, Linux, R

- Note: The code present in this repository is only for the part which we developed. So, it has to plugged in into the core Jnomics framework to build and use it. Here is the link for Jnomics: http://sourceforge.net/apps/mediawiki/jnomics/index.php?title=Jnomics