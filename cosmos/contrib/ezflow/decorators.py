import pprint,sys
import types
from inspect import getargspec
import inspect
import decorator

#part of inspect.method which is not in python 2.6
def ismethod(object):
    """Return true if the object is an instance method.

    Instance method objects provide these attributes:
        __doc__         documentation string
        __name__        name with which this method was defined
        im_class        class object in which this method belongs
        im_func         function object containing implementation of method
        im_self         instance to which this method is bound, or None"""
    return isinstance(object, types.MethodType)    

#inspect.callargs is not part of 2.6 so i copied it here
def getcallargs(func, *positional, **named):
    """Get the mapping of arguments to values.

    A dict is returned, with keys the function argument names (including the
    names of the * and ** arguments, if any), and values the respective bound
    values from 'positional' and 'named'."""
    args, varargs, varkw, defaults = getargspec(func)
    f_name = func.__name__
    arg2value = {}

    # The following closures are basically because of tuple parameter unpacking.
    assigned_tuple_params = []
    def assign(arg, value):
        if isinstance(arg, str):
            arg2value[arg] = value
        else:
            assigned_tuple_params.append(arg)
            value = iter(value)
            for i, subarg in enumerate(arg):
                try:
                    subvalue = next(value)
                except StopIteration:
                    raise ValueError('need more than %d %s to unpack' %
                                     (i, 'values' if i > 1 else 'value'))
                assign(subarg,subvalue)
            try:
                next(value)
            except StopIteration:
                pass
            else:
                raise ValueError('too many values to unpack')
    def is_assigned(arg):
        if isinstance(arg,str):
            return arg in arg2value
        return arg in assigned_tuple_params
    if ismethod(func) and func.im_self is not None:
        # implicit 'self' (or 'cls' for classmethods) argument
        positional = (func.im_self,) + positional
    num_pos = len(positional)
    num_total = num_pos + len(named)
    num_args = len(args)
    num_defaults = len(defaults) if defaults else 0
    for arg, value in zip(args, positional):
        assign(arg, value)
    if varargs:
        if num_pos > num_args:
            assign(varargs, positional[-(num_pos-num_args):])
        else:
            assign(varargs, ())
    elif 0 < num_args < num_pos:
        raise TypeError('%s() takes %s %d %s (%d given)' % (
            f_name, 'at most' if defaults else 'exactly', num_args,
            'arguments' if num_args > 1 else 'argument', num_total))
    elif num_args == 0 and num_total:
        if varkw:
            if num_pos:
                # XXX: We should use num_pos, but Python also uses num_total:
                raise TypeError('%s() takes exactly 0 arguments '
                                '(%d given)' % (f_name, num_total))
        else:
            raise TypeError('%s() takes no arguments (%d given)' %
                            (f_name, num_total))
    for arg in args:
        if isinstance(arg, str) and arg in named:
            if is_assigned(arg):
                raise TypeError("%s() got multiple values for keyword "
                                "argument '%s'" % (f_name, arg))
            else:
                assign(arg, named.pop(arg))
    if defaults:    # fill in any missing values with the defaults
        for arg, value in zip(args[-num_defaults:], defaults):
            if not is_assigned(arg):
                assign(arg, value)
    if varkw:
        assign(varkw, named)
    elif named:
        unexpected = next(iter(named))
        if isinstance(unexpected, unicode):
            unexpected = unexpected.encode(sys.getdefaultencoding(), 'replace')
        raise TypeError("%s() got an unexpected keyword argument '%s'" %
                        (f_name, unexpected))
    unassigned = num_args - len([arg for arg in args if is_assigned(arg)])
    if unassigned:
        num_required = num_args - num_defaults
        raise TypeError('%s() takes %s %d %s (%d given)' % (
            f_name, 'at least' if defaults else 'exactly', num_required,
            'arguments' if num_required > 1 else 'argument', num_total))
    return arg2value    

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
    except KeyError:
        print >> sys.stderr, "Format Error:"
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
            print "@fromtags Error: {0} does not exist in {1}.tags which is {2}".format(t,instance,instance.tags)
            raise
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
    