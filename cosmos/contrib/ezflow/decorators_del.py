import pprint,sys
import inspect
import decorator

def cosmos_format(s,d):
    """
    Formats string s with d.  If there is an error, print helpful messages .
    
    
    >>> class X(object):
    >>>     @pformat
    >>>     def x(self,p1,p2):
    >>>         return "p1={p1} p2={p2}"
    >>> print x('val1','val2')
    p1=val1 p2=val2
    >>>
    
    """
    if not isinstance(s,str):
        raise Exception('Wrapped function must return a str')
    try:
        return s.format(**d)
    except (KeyError,IndexError) as e:
        print >> sys.stderr, "Format Error: {0}".format(e)
        print >> sys.stderr, "\tTried to format: {0}".format(pprint.pformat(s))
        print >> sys.stderr, "\tWith: {0}".format(pprint.pformat(d))
        raise

class PFormatError(Exception): pass

@decorator.decorator
def pformat(func,*args,**kwargs):
    callargs = getcallargs(func,*args,**kwargs)
    if 'self' in callargs: del callargs['self']
    r = func(*args,**kwargs)
    try:
        return cosmos_format(r,callargs)
    except TypeError as e:
        raise PFormatError("{0} - callargs = {1}".format(e,callargs))
    

#class X(object):
#    @pformat
#    def x(self,p1,p2='val'):
#        return "p1={p1} p2={p2}"
#print X().x('val1','val2')

class FromTagsError(Exception):pass
def fromtags(*tags):
    """
    Fills in a method's parameter's value with the keys from the method's instance's tags listed.
    
    >>> class X(object):
    >>>     tags = {'param1':'worked'}
    >>>     @from_tags('param1')
    >>>     def x(self,param1='fail',param2='fail2'):
    >>>         print "{0} {1}".format(param1,param2)
    >>>          
    >>> X().x(param2='apple')
    worked apple
    >>>
    """
    @decorator.decorator
    def wrapped(fxn,*args,**kwargs):
        if not hasattr(fxn, '__call__'): 
            raise Exception('@from_tags requires you to specify tag names in the argument list')
        instance = args[0]
        callargs = getcallargs(fxn,*args,**kwargs)
        try:
            for t in tags: callargs[t] = instance.tags[t]
        except KeyError:
            raise FromTagsError("@fromtags Error: {0} does not exist in {1}.tags which is {2}".format(t,instance,instance.tags))
        return pformat(fxn)(**callargs)
    
    return wrapped

class DecException(Exception): pass

@decorator.decorator
def opoi(func,*args,**kwargs):
    """
    One parent one input.
    Useful when specifying a cmd that only supports a single parent and single input.  Using @opoi will cause the first parameter
    to be transformed into it's first index.
    
    .. note:: this will also pformat the output.  This is done because otherwise pformat gets automatically called with the original parameter as an input
    list
    
    >>> class X(object):
    >>>     @one_parent
    >>>     def x(input_param):
    >>>         return input_param
    >>> 
    >>> x(['Hello World'])
    >>> Hello World # Not ['Hello World']
    
    """
    first_arg = inspect.getargspec(func)[0][1]
    callargs = getcallargs(func,*args,**kwargs)
    try:
        callargs[first_arg] = callargs[first_arg][0]
    except IndexError as e:
        raise DecException("{0}. - Does the command have at least one input? args = {1} kwargs = {2}".format(e, args, kwargs))
    return pformat(func)(**callargs)

#class X(object):
#    def x(self,paramA,param1):
#        return '{paramA} {param1}'
#    
#i = X()
#print pformat(i.x.im_func)(i,**{'paramA':[1],'param1':2})
#print opoi(pformat(i.x.im_func))(i,**{'paramA':[1],'param1':2})
    