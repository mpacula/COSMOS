DRMAA_LIBRARY_PATH=/opt/lsf/7.0/linux2.6-glibc2.3-x86_64/lib/libdrmaa.so
GATK_DIR=~/tools/GenomeAnalysisTKLite-2.1-8-gbb7f038
QUEUE_DIR=~/tools/QueueLite-2.1-8-gbb7f038
BUNDLE_DIR=/nas/erik/gatk_bundle1.5
CLASSPATH=/opt/sge6/lib/linux-x64:$GATK_DIR
OUTPUT_DIR=~/tmp
WORKFLOW_DIR=~/workspace/Cosmos/my_workflows/gatk
INPUT_FILE=$WORKFLOW_DIR/bam.list
java \
	-classpath $CLASSPATH \
	-Xmx4g \
	-Djava.io.tmpdir=/nas/erik/tmp \
	-jar $QUEUE_DIR/QueueLite.jar \
	-S ~/workspace/Cosmos/my_workflows/gatk/DataProcessingPipeline.scala \
	-i $INPUT_FILE \
	-R $BUNDLE_DIR/human_g1k_v37.fasta \
	-D $BUNDLE_DIR/dbsnp_135.b37.vcf \
	-outputDir $OUTPUT_DIR/ \
	-p MGH_BC \
	-gv $OUTPUT_DIR/graphviz.dot \
	-gvsg $OUTPUT_DIR/graphviz_scatter_gather.dot \
	-log $OUTPUT_DIR/queue_output.log \
	-jobReport $OUTPUT_DIR/job_report.pdf \
	-retry 5 \
	-resMemReq 3 \
	-run
