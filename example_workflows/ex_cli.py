from cosmos import session, Workflow, ToolGraph, INPUT, cli


def main(workflow, input_file, **kwargs):
    g = ToolGraph()
    g.source([INPUT(input_file, tags={'i': 1}, fmt='dir')])


if __name__ == '__main__':
    import argparse

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('input_file')
    cli.add_workflow_args(p)
    p.set_defaults(func=main)

    wf, kwargs = cli.parse_args(p)
    kwargs['func'](wf, **kwargs)