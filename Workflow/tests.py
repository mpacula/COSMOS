from Workflow.models import Workflow, Batch, Node
from django.test import TestCase
from django.core.exceptions import ValidationError


class Test_Workflow(TestCase):
    def setUp(self):
        self.wF = Workflow.__create(name='Test_WF',root_output_dir='/tmp')
        
    def tearDown(self):
        self.wF.delete()
        
    def test_no_duplicate_batch_names(self):
        self.wF.add_batch(name='Test_Batch')
        #import ipdb; ipdb.set_trace()
        self.assertRaises(ValidationError,self.wF.add_batch,name='Test_Batch')
        
    def test_no_duplicate_node_names(self):
        b = self.wF.add_batch(name='Test_Batch')
        b.add_node(pre_command='',outputs='',name='test_node')
        self.assertRaises(ValidationError,b.add_node,pre_command='',outputs='',name='test_node')
    
    
    def test_one_command(self):
        b = self.wF.add_batch(name='Test_Batch')
        b.add_node(pre_command='ls / > {output_dir}/{outputs[output_file]}',outputs={'output_file':'myls.out'},name='ls_test1')
        self.wF.run_batch(b)
        
    def test_resume(self):
        #run once
        b = self.wF.add_batch(name='Test_Batch')
        b.add_node(pre_command='ls / > {output_dir}/{outputs[output_file]}',outputs={'output_file':'myls.out'},name='ls_test1')
        self.wF.run_batch(b)
 
        #run second time, have to setup again
        self.wF  = Workflow.__resume(name='Test_WF')
        assert self.wF.batches.count() == 1
        
        #next command shouldn't __create a new node since it already exists
        b = self.wF.add_batch(name='Test_Batch')
        b.add_node(pre_command='ls / > {output_dir}/{outputs[output_file]}',outputs={'output_file':'myls.out'},name='ls_test1')
        
        assert self.wF.batches.count() == 1
        #next command should skip execution
        self.wF.run_batch(b)
         



#from JobManager.models import JobManager,Job
#from Tools.models.Echo import Echo
#from Tools.models.Cat import Cat
#from Workflow.models import Workflow
#from django.test import LiveServerTestCase
#
#def slow(f):
#  def decorated(self):
#    f(self) #comment to skip slow functions
#    pass 
#  return decorated
#
#
#class Test_Echo(LiveServerTestCase):
#    def setUp(self):
#        self.JM = JobManager.objects.__create()
#        self.JM.init()
#        self.JM.save()
#        self.WF = Workflow.objects.__create()
#        self.WF.save()
#        
#    def tearDown(self):
#        self.JM.close()
#        
#    def test_add_node(self):
#        echo = Echo(text="test")
#        echo.save()
#        cat = Cat()
#        cat.save()
#        self.WF.add_node(cat)
#        self.WF.add_node(echo)
#        nodes = self.WF.get_nodes()
#        assert nodes[0] == echo
#        assert nodes[1] == cat
#        assert len(nodes) == 2
#        assert echo in nodes
#        assert cat in nodes
#        
#    
#    def test_add_edge(self):
#        echo = Echo(text="test")
#        echo.save()
#        cat = Cat()
#        cat.save()
#        self.WF.add_edge(echo,'output_file',cat,'input_file')
#        #check edges
#        edges = self.WF._DAG.edges(data=True)
#        assert edges[0][0] == echo
#        assert edges[0][1] == cat
#        assert edges[0][2] == {'source_field': u'output_file', 'destination_field': u'input_file'}
#        #check nodes
#        nodes = self.WF.get_nodes()
#        nodes = self.WF.get_nodes()
#        assert len(nodes) == 2
#        assert echo in nodes
#        assert cat in nodes
#        import ipdb; ipdb.set_trace()
        
    
        