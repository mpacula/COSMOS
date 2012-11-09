from infix import G,m,s,g


class ALN:
    inputs = ['fastq']
    outputs = ['fastq','sai']
    
    def run(self,fastq,sample,lane,flowcell):
        return "bwa aln $in.fastq > $outs.sai"

class SAMPE:
    inputs = [['fastq','sai']]
    def run(self,inputs,sample,lane,flowcell):
        return "bwa sampe -r \"sample,lane,flowcell\" $in.1.fastq $in.2.fastq $in.1.sai $in.2.sai > $out.bam"

class CLEAN:
    def run(self,bams):
        return "clean bams[0] -o $out.bam"

class RMDUP:
    def run(self,bams):
        return "clean bams[0] -o $out.bam"

class ITRC:
    inputs = ['bam']
    outputs = ['bam','targets']
    def run(self,bam,interval,):
        "itrc bams[0] -I {interval} -o $out.targets"

class IR:
    inputs = ['bam']
    def run(self,bam,targets,config):
        "IR"

class BQSR:
    def run(self):
        pass
        
    
interval = ('interval',[1,2,3,4,'X','Y'])
glm = ('glm',['SNP','INDEL'])

#_INPUT = None
#_INPUT |m| ALN |g| ['fq_pair'] |m| SAMPE |m| CLEAN |g| ['sample'] |m| RMDUP |s| interval |m| ITRC |m| IR |s| [interval] |m| BQSR |m| PR |s| [chr,glm] |m| UG / ['interval'] |m| CV |m| VQR |m| AVQR / ['glm'] |m| CV
#G.edge
#config = { } #parameters
#print workflow
#workflow.run(config)
