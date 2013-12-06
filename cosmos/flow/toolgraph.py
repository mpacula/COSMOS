import itertools as it
import re

import networkx as nx

from cosmos.utils.helpers import groupby, get_all_dependencies, validate_is_type_or_list
from cosmos.models import Task, TaskError
from cosmos.flow.tool import Tool, INPUT


class Relationship(object):
    """Abstract Class for the various rel strategies"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        m = re.search("^(\w).+2(\w).+$", type(self).__name__)
        return '{0}2{1}'.format(m.group(1), m.group(2))


class one2one(Relationship):
    pass


class many2one(Relationship):
    def __init__(self, keywords, *args, **kwargs):
        assert isinstance(keywords, list), '`keywords` must be a list'
        self.keywords = keywords
        super(Relationship, self).__init__(*args, **kwargs)


class one2many(Relationship):
    def __init__(self, split_by, *args, **kwargs):
        assert isinstance(split_by, list), '`split_by` must be a list'
        if len(split_by) > 0:
            assert isinstance(split_by[0], tuple), '`split_by` must be a list of tuples'
            assert isinstance(split_by[0][0], str), 'the first element of tuples in `split_by` must be a str'
            assert isinstance(split_by[0][1],
                              list), 'the second element of the tuples in the `split_by` list must also be a list'

        self.split_by = split_by
        super(Relationship, self).__init__(*args, **kwargs)


class ToolGraph(object):
    """
    A Representation of a workflow as a :term:`ToolGraph` of jobs.
    """

    def __init__(self, cpu_req_override=False, mem_req_factor=1):
        """
        :param cpu_req_override: set to an integer to override all task cpu_requirements.  Useful when a :term:`DRM` does not support requesting multiple cpus
        :param mem_req_factor: multiply all task mem_reqs by this number.
        :param dag.ignore_stage_name_collisions:  Allows the flowfxns to add to stages that already exists.
        """
        self.tool_G = nx.DiGraph()
        self.stage_G = nx.DiGraph()
        self.cpu_req_override = cpu_req_override
        self.mem_req_factor = mem_req_factor

    def input(self, inputs, name=None):
        assert isinstance(inputs[0], INPUT)
        for i,inp in enumerate(inputs):
            if len(inp.tags)==0:
                inp.tags['input'] = i

        return self.source(inputs, name)

    def source(self, tools, name=None):
        if name is None:
            name = tools[0].name
        assert isinstance(tools, list), 'tools must be a list'
        tags = [tuple(t.tags.items()) for t in tools]
        assert len(tags) == len(
            set(tags)), 'Duplicate inputs tags detected for {0}.  Tags within a stage must be unique.'.format(INPUT)

        stage = Stage(tool=type(tools[0]), tools=tools, parents=[], rel=None, name=name, is_source=True)
        for tool in stage.tools:
            tool.stage = stage

        self.stage_G.add_node(stage)

        return stage


    def stage(self, tool, parents, rel=one2one, name=None, extra_tags=None):
        """
        Creates a Stage in this TaskGraph
        """
        stage = Stage(tool, parents, rel, name, extra_tags)
        assert stage.name not in [n.name for n in self.stage_G.nodes()], 'Duplicate stage names detected: {0}'.format(
            stage.name)

        self.stage_G.add_node(stage)
        for parent in stage.parents:
            self.stage_G.add_edge(parent, stage)

        return stage

    def resolve(self, settings={}, parameters={}):
        self._resolve_tools()
        self.configure(settings, parameters)
        return self

    def _add_tool_to_tool_G(self, new_tool, parents=None):
        if parents is None:
            parents = []
        assert new_tool.tags not in [t.tags for t in self.tool_G.nodes() if
                                     t.stage == new_tool.stage], 'Duplicate set of tags detected in {0}'.format(
            new_tool.stage)

        self.tool_G.add_node(new_tool)
        for p in parents:
            self.tool_G.add_edge(p, new_tool)

    def _resolve_tools(self):
        for stage in nx.topological_sort(self.stage_G):
            if stage.is_source:
                #stage.tools is already set
                for tool in stage.tools:
                    self._add_tool_to_tool_G(tool)

            elif isinstance(stage.rel, one2one):
                for parent_tool in it.chain(*[s.tools for s in stage.parents]):
                    tags2 = parent_tool.tags.copy()
                    tags2.update(stage.extra_tags)
                    new_tool = stage.tool(stage=stage, dag=self, tags=tags2)
                    stage.tools.append(new_tool)
                    self._add_tool_to_tool_G(new_tool, [parent_tool])

            elif isinstance(stage.rel, many2one):
                keywords = stage.rel.keywords
                if type(keywords) != list:
                    raise TypeError('keywords must be a list')
                if any(k == '' for k in keywords):
                    raise TypeError('keyword cannot be an empty string')

                parent_tools = list(it.chain(*[s.tools for s in stage.parents]))
                parent_tools_without_all_keywords = filter(lambda t: not all([k in t.tags for k in keywords]),
                                                           parent_tools)
                parent_tools_with_all_keywords = filter(lambda t: all(k in t.tags for k in keywords), parent_tools)

                if len(
                        parent_tools_with_all_keywords) == 0: raise RelationshipError, 'Parent stages must have at least one tool with all many2one keywords of {0}'.format(
                    keywords)

                for tags, parent_tool_group in groupby(parent_tools_with_all_keywords,
                                                       lambda t: dict((k, t.tags[k]) for k in keywords if k in t.tags)):
                    parent_tool_group = list(parent_tool_group) + parent_tools_without_all_keywords
                    tags.update(stage.extra_tags)
                    new_tool = stage.tool(stage=stage, dag=self, tags=tags)
                    stage.tools.append(new_tool)
                    self._add_tool_to_tool_G(new_tool, parent_tool_group)

            elif isinstance(stage.rel, one2many):
                parent_tools = list(it.chain(*[s.tools for s in stage.parents]))
                #: splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
                splits = [list(it.product([split[0]], split[1])) for split in stage.rel.split_by]
                for parent_tool in parent_tools:
                    for new_tags in it.product(*splits):
                        tags = dict(parent_tool.tags).copy()
                        tags.update(stage.extra_tags)
                        tags.update(dict(new_tags))
                        new_tool = stage.tool(stage=stage, dag=self, tags=tags)
                        stage.tools.append(new_tool)
                        self._add_tool_to_tool_G(new_tool, [parent_tool])

            else:
                raise AssertionError, 'Stage constructed improperly'

        for tool in self.tool_G:
            for key in tool.tags:
                if not re.match('\w', key):
                    raise ValueError("{0}.{1}'s tag's keys are not alphanumeric: {3}".format(stage,tool, tool.tags))


        return self

    def as_image(self, resolution='stage', save_to=None):
        """
        Writes the :term:`ToolGraph` as an image.
        gat
        :param path: the path to write to
        """
        import pygraphviz as pgv

        dag = pgv.AGraph(strict=False, directed=True, fontname="Courier", fontsize=11)
        dag.node_attr['fontname'] = "Courier"
        dag.node_attr['fontsize'] = 8
        dag.graph_attr['fontsize'] = 8
        dag.edge_attr['fontcolor'] = '#586e75'
        #dag.node_attr['fontcolor']='#586e75'
        dag.graph_attr['bgcolor'] = '#fdf6e3'

        if resolution == 'stage':
            dag.add_nodes_from([n.label for n in self.stage_G.nodes()])
            for u, v, attr in self.stage_G.edges(data=True):
                if isinstance(v.rel, many2one):
                    dag.add_edge(u.label, v.label, label=v.rel, style='dotted', arrowhead='odiamond')
                elif isinstance(v.rel, one2many):
                    dag.add_edge(u.label, v.label, label=v.rel, style='dashed', arrowhead='crow')
                else:
                    dag.add_edge(u.label, v.label, label=v.rel, arrowhead='vee')
        elif resolution == 'tool':
            dag.add_nodes_from(self.tool_G.nodes())
            dag.add_edges_from(self.tool_G.edges())
            for stage, tools in groupby(self.tool_G.nodes(), lambda x: x.stage):
                sg = dag.add_subgraph(name="cluster_{0}".format(stage), label=stage.label, color='lightgrey')
        else:
            raise TypeError, '`resolution` must be `stage` or `tool'

        dag.layout(prog="dot")
        return dag.draw(path=save_to, format='svg')

    def configure(self, settings={}, parameters={}):
        """
        Sets the parameters an settings of every tool in the dag.

        :param parameters: (dict) {'stage_name': { 'name':'value', ... }, {'stage_name2': { 'key':'value', ... } }
        :param settings: (dict) { 'key':'val'} }
        """
        self.parameters = parameters
        for tool in self.tool_G.node:
            tool.settings = settings
            if tool.stage.name not in self.parameters:
                #set defaults, then override with parameters
                self.parameters[tool.stage.name] = tool.default_params.copy()
                self.parameters[tool.stage.name].update(parameters.get(tool.__class__.__name__, {}))
                self.parameters[tool.stage.name].update(parameters.get(tool.stage.name, {}))
            tool.parameters = self.parameters.get(tool.stage.name, {})
        return self

    def add_to_workflow(self, workflow):
        """
        Add this dag to a workflow.  Only adds tools to stages that are new, that is, another tag in the same
        stage with the same tags does not already exist.

        :param workflow: the workflow to add to
        """
        workflow.log.info('Adding tasks to workflow.')

        ### Validation
        taskfiles = list(it.chain(*[n.output_files for n in self.tool_G.nodes()]))

        # check paths for duplicates.
        # TODO this should also be done at the DB level since resuming can add taskfiles with the same path
        paths = [tf.path for tf in taskfiles if tf.path]
        if len(paths) != len(set(paths)):
            import pprint

            raise DAGError(
                'Multiple taskfiles refer to the same path.  Paths should be unique. taskfile.paths are:{0}'.format(
                    pprint.pformat(sorted(paths))))

        ### Add stages, and set the tool.stage reference for all tools

        # Load stages or add if they don't exist
        django_stages = {}
        for stage in nx.topological_sort(self.stage_G):
            django_stages[stage.name] = workflow.add_stage(stage.name)

        # Get the tasks associated with the tools that already successful
        stasks = list(workflow.tasks.select_related('_output_files', 'stage'))


        ### Validation
        should_be_unique = [(tool.stage, tuple(tool.tags.items())) for tool in self.tool_G.nodes()]
        assert len(should_be_unique) == len(set(should_be_unique)), 'A stage has tools with duplicate tags'

        # Figure out which tools have already executed successfully
        tools_successful_already = []
        for tool, task in intersect_tool_task_graphs(self.tool_G.nodes(), stasks):
            tool._task_instance = task
            tools_successful_already.append(tool)
        tools_not_yet_successful = filter(lambda tool: tool not in tools_successful_already, self.tool_G.nodes())

        # Figure out which tools have had their dependencies change
        changed_dependencies = get_all_dependencies(self.tool_G, tools_not_yet_successful, include_source=False)
        successful_changed_dependencies = filter(lambda tool: tool in tools_successful_already, changed_dependencies)

        # Delete tasks who's dependencies have changed
        # TODO delete tasks that are in DB but not in toolgraph?
        delete_tasks = []
        for tool in successful_changed_dependencies:
            delete_tasks.append(tool._successful_task)
            del tool._task_instance
            tools_not_yet_successful.append(tool)
            tools_successful_already.remove(tool)

        if len(delete_tasks):
            workflow.bulk_delete_tasks(delete_tasks)

        # Set output_files of tool if they are already in the DB
        for tool in tools_successful_already:
            tool.output_files = tool._task_instance.output_files


        ### Save new tools to DB

        #bulk save tasks
        new_nodes = tools_not_yet_successful
        workflow.log.info(
            'Total tasks: {0}, New tasks being added: {1}'.format(len(self.tool_G.nodes()), len(new_nodes)))

        #bulk save task_files.  All inputs have to at some point be an output, so just bulk save the outputs.
        #Must come before adding tasks, since taskfile.ids must be populated to compute the proper pcmd.
        taskfiles = list(it.chain(*[n.output_files for n in new_nodes]))
        workflow.bulk_save_taskfiles(taskfiles)

        #bulk save tasks
        for node in new_nodes:
            node._task_instance = self.__new_task(django_stages[node.stage.name], node)
        tasks = [node._task_instance for node in new_nodes]
        workflow.bulk_save_tasks(tasks)

        ### Bulk add task->output_taskfile relationships
        ThroughModel = Task._output_files.through
        rels = [ThroughModel(task_id=n._task_instance.id, taskfile_id=tf.id) for n in new_nodes for tf in
                n.output_files]
        ThroughModel.objects.bulk_create(rels)

        ### Bulk add task->input_taskfile relationships
        ThroughModel = Task._input_files.through
        rels = [ThroughModel(task_id=n._task_instance.id, taskfile_id=tf.id) for n in new_nodes for tf in n.input_files]

        ThroughModel.objects.bulk_create(rels)


        ### Bulk add task->parent_task relationships
        ThroughModel = Task._parents.through
        new_edges = filter(lambda e: e[0] in new_nodes or e[1] in new_nodes, self.tool_G.edges())
        rels = [ThroughModel(from_task_id=child._task_instance.id,
                             to_task_id=parent._task_instance.id)
                for parent, child in new_edges]
        ThroughModel.objects.bulk_create(rels)

        workflow.stage_graph = self.as_image(resolution='stage')
        workflow.save()

    def add_run(self, workflow, finish=True):
        """
        Shortcut to add to workflow and then run the workflow
        :param workflow: the workflow this dag will be added to
        :param finish: pass to workflow.run()
        """
        self.add_to_workflow(workflow)
        workflow.run(finish=finish)


    def __new_task(self, django_stage, tool):
        """
        Instantiates a task from a tool.  Assumes TaskFiles already have real primary keys.

        :param django_stage: The Stage (of the django model type) that the task should belong to.
        :param tool: The Tool.
        """

        try:
            return Task(
                stage=django_stage,
                pcmd=tool.pcmd,
                tags=tool.tags,
                memory_requirement=tool.mem_req * self.mem_req_factor,
                cpu_requirement=tool.cpu_req if not self.cpu_req_override else self.cpu_req_override,
                time_requirement=tool.time_req,
                NOOP=tool.NOOP,
                succeed_on_failure=tool.succeed_on_failure)
        except TaskError as e:
            raise TaskError('{0}. Task is {1}.'.format(e, tool))


class Stage():
    def __init__(self, tool=None, parents=None, rel=None, name=None, extra_tags=None, tools=None, is_source=False):
        if parents is None:
            parents = []
        if tools is None:
            tools = []
        if tools and tool and not is_source:
            raise TypeError, 'cannot initialize with both a `tool` and `tools` unless `is_source`=True'
        if extra_tags is None:
            extra_tags = {}
        if rel == one2one or rel is None:
            rel = one2one()

        assert issubclass(tool, Tool), '`tool` must be a subclass of `Tool`'
        # assert rel is None or isinstance(rel, Relationship), '`rel` must be of type `Relationship`'

        self.tool = tool
        self.tools = tools
        self.parents = validate_is_type_or_list(parents, Stage)
        self.rel = rel
        self.is_source = is_source

        self.extra_tags = extra_tags
        self.name = name or self.tool.__name__

        if not re.search(r"^\w+$", self.name):
            raise ValueError, 'Stage name `{0}` must be alphanumeric'.format(self.name)


    @property
    def label(self):
        return '{0} (x{1})'.format(self.name, len(self.tools))

    def __str__(self):
        return '<Stage {0}>'.format(self.name)


def intersect_tool_task_graphs(tools, tasks):
    intersection = []
    for tpl, group in groupby(tools + tasks, lambda x: (x.tags, x.stage.name)):
        group = list(group)
        assert len(group) < 3, 'Database integrity error'
        if len(group) == 2:
            tool = group[0] if isinstance(group[1], Task) else group[1]
            task = group[0] if isinstance(group[0], Task) else group[1]
            intersection.append((tool, task))
    return intersection


class DAGError(Exception): pass


class StageNameCollision(Exception): pass


class FlowFxnValidationError(Exception): pass


class RelationshipError(Exception): pass
