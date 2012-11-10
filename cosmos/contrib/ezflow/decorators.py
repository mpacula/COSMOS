import functools
import pprint,sys

class pformat(object):
    """
    Parameter Format. format()s the output of a function with its own parameters as keywords.  Output of wrapped function
    must be a str.
    
    >>> @pformat
    >>> def x(p1,p2='val'2):
    >>>     return "p1={p1} p2={p2}"
    >>> print x('val1','val2')
    >>> p1=val1 p2=val2
    
    """
    def __init__(self,function):
        self.f = function

    def __call__(self, *args, **kwargs):
        argnames = self.f.func_code.co_varnames[:self.f.func_code.co_argcount]
        params = dict([ param for param in zip(argnames,args) + kwargs.items() ])
        r = self.f(*args, **kwargs)
        try:
            return r.format(**params)
        except KeyError:
            print >> sys.stderr, "Format Error:"
            print >> sys.stderr, "\tTried to format: {0}".format(pprint.pformat(r))
            print >> sys.stderr, "\tWith: {0}".format(pprint.pformat(params))
            raise
    
    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)

#def from_tags(method_to_decorate):
#    def wrapper(self, *args, **kwargs):
#        print self
#        print args
#        
#        print self.tags
#        return method_to_decorate(self, )
#    return wrapper
#
#class Blah(object):
#    tag = 'yay'
#    @from_tags('param')
#    def x(self,a,param=3):
#        pass
#
#print Blah().x(a=3)