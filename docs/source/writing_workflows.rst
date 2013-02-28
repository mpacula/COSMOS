.. _writing_workflows:

Writing Workflows
==================

The easiest way to write a workflow is to use the *ezflow* package.

.. seealso::
    You may want to view some basic examples first.
    :ref:`examples`

.. py:module:: cosmos.contrib.ezflow

EZFlow
============

*EZFlow* is a package that makes creating workflows easy.  It allows you to define classes that represent
a command line tool and various functions to make creating a complex workflow of jobs represented by a
:term:`DAG` simple.

A `DAG` consists of Stages, Tools and Tool dependencies.
Defining Tools
--------------
A tool represents an executable (like echo, cat, or paste, or script) that is run from the command line.
A tool is a class that overrides :py:class:`~tool.Tool`, and defines :py:meth:`~tool.Tool.cmd`,
(unless the tool doesn't actually perform an operation, ie
:py:attr:`tool.Tool.NOOP` == True).

.. code-block:: python

    from cosmos.contrib.ezflow.tool import Tool

    class WordCount(Tool):
        name = "Word Count"
        inputs = ['txt']
        outputs = ['txt']
        mem_req = 1*1024
        cpu_req = 1

        def cmd(self,i,s,p):
            return r"""
                wc {i[txt][0]} > $OUT.txt
                """

This tool will read a txt file, count the number of words, and write it to another text file.


See the :py:class:`Tool API <tool.Tool>` for more properties that can be overridden to obtain
various behaviors.

Defining Input Files
--------------------

An Input file is an instantiation of :py:class:`tool.INPUT`, which is just a Tool with
:py:attr:`tool.INPUT.NOOP` set to True, and a way to initialize its output files as existing paths on the filesystem.

Here's an example of how to create an instance of an :py:class:`tool.INPUT` File:

.. code-block:: python

    from cosmos.contrib.ezflow import INPUT

    input_file = INPUT('/path/to/file.txt')

``input_file`` will now be a tool instance with an output file called 'txt' that points to :file:`/path/to/file.txt`.

A more fine grained approach to defining input files:

.. code-block:: python

    from cosmos.Workflow.models import TaskFile
    from cosmos.contrib.ezflow import INPUT
    INPUT(taskfile=TaskFile(name='favorite_txt',path='/path/to/favorite_txt.txt.gz',fmt='txt.gz'),tags={'color':'red'})

Designing Workflows
===================

All jobs and and job dependencies are represented by the :py:class:`dag.DAG` class.

There are 5 infix operators you can use to generate a DAG.  They each take an
instance of a :py:class:`~dag.DAG` on the left, and apply the :py:class:`tool.Tool` class
on the right to the last :py:class:`cosmos.Workflow.models.Stage` added to the ``DAG``.

.. hint::

    You can always visualize the ``DAG`` that you've built using :py:meth:`dag.DAG.create_dag_img`.
    (see :ref:`examples` for details)

*The 5 infix operators are:*

.. py:method:: |Add|

    Always the first operator of a workflow.  Simply the list of tool instances in `tools` to the dag, without adding
    any dependencies.

    :param tools: (list of tools) A list of tool instances to add.
    :param stage_name: (str) The name of the stage.  Defaults to the tool_class.name.
    :returns: The modified dag.

    >>> dag() |Add| [tool1,tool2,tool3,tool4]
    >>> dag() |Add| ([tool1,tool2,tool3,tool4],'My Stage Name')

.. py:method:: |Map|

    Creates a one2one relationships for each tool in the stage last added to the dag, with a new tool of
    type ``tool_class``.

    :param tool_class: (subclass of Tool)
    :param stage_name: (str) The name of the stage.  Defaults to the tool_class.name.
    :returns: The modified dag.

    >>> dag() |Map| Tool_Class

.. py:method:: |Split|

    Creates one2many relationships for each tool in the stage last added to the dag, with every possible combination
    of keywords in split_by.  New tools will be of class ``tool_class`` and tagged with one of the possible keyword
    combinations.

    :param tool_class: (subclass of Tool)
    :param split_by: (list of (str,list)) Tags to split by.
    :param stage_name: (str) The name of the stage.  Defaults to the tool_class.name.
    :returns: The modified dag.

    >>> dag() |Split| ([('shape',['square','circle']),('color',['red','blue'])],Tool_Class)
    >>> dag() |Split| ([('shape',['square','circle']),('color',['red','blue'])],Tool_Class,'My Stage Name')

    The above will generate 4 new tools dependent on each tool in the most recent stage.  The new tools will be tagged
    with:

    .. code-block:: python

        {'shape':'square','color':'red'}, {'shape':'square','color':blue'},
        {'shape':'circle','color':'red'}, {'shape':'square','circle':blue'}

.. py:method:: |Reduce|

    Creates many2one relationships for each tool in the stage last added to the dag grouped by ``keywords``,
    with a new tool of type ``tool_class``.

    :param keywords: (list of str) Tags to reduce to.  All keywords not listed will not be passed on to the tasks generated.
    :param tool_class: (subclass of Tool)
    :param stage_name: (str) The name of the stage.  Defaults to the tool_class.name.
    :returns: The modified dag.

    >>> dag() |Reduce| (['shape','color'],Tool_Class)

    In the above example, the most recent stage will be grouped into tools with the same `shape` and `color`, and a
    dependent tool of type ``tool_class`` will be created tagged with the `shape` and `color` of their parent group.

.. py:method:: |ReduceSplit|

    Creates many2one relationships for each tool in the stage last added to the dag grouped by ``keywords`` and split
    by the product of ``split_by``,
    with a new tool of type ``tool_class``.

    :param keywords: (list of str) Tags to reduce to.  All keywords not listed will not be passed on to the tasks generated.
    :param split_by: (list of (str,list)) Tags to split by.
    :param tool_class: (subclass of Tool)
    :param stage_name: (str) The name of the stage.  Defaults to the tool_class.name.
    :returns: The modified dag.

    >>> dag() |ReduceSplit| (['color','shape'],[('size',['small','large'])],Tool_Class)

    The above example will group the last stage into tools with the same `color` and `shape`, and create
    two new dependent tools with tags ``{'size':'large'}`` and ``{'size':'small'}``, plus the ``color`` and ``shape``
    of their parents.



EZFlow API
===========

Tool
-----

.. automodule:: cosmos.contrib.ezflow.tool
    :members:


DAG
-----

.. automodule:: cosmos.contrib.ezflow.dag
    :private-members:
    :members:
    :undoc-members:

