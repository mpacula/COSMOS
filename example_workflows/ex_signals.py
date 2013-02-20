"""
This shows how to use signals to run arbitrary code when a stage fails.  One could easily
modify this to send SMS texts instead of e-mails.

.. note:: that signals for status changes are not sent when they're changed due to a
workflow starting, or a workflow terminating.
"""
####################
# CLI
####################

from cosmos.Workflow.cli import CLI

cli = CLI()
cli.parser.add_argument('-e', '--email', type=str, help='Email address to report messages to.',
                        required=True)
WF = cli.parse_args() # parses command line arguments
email = cli.parsed_kwargs['email']

####################
# Signals
####################

from cosmos.Workflow import signals
from django.dispatch import receiver
import smtplib

# Send an e-mail each time the workflow fails
# For more details on using signals,
# See the `Django Signals Documentation <https://docs.djangoproject.com/en/dev/topics/signals/>`_
#
# .. note:: I have not tested the actual e-mailing code, but the signal does work.
#
@receiver(signals.stage_status_change)
def email_on_fail(sender, status, **kwargs):
    stage = sender
    WF.log.warning('Notifying {0} of stage failure.'.format(email))
    if status == 'failed':
        SUBJECT = "{0} Failed!".format(stage)
        TO = email
        FROM = "me@me.com"
        HOST = "smpt.server"
        text = '{0} has failed at stage {1}'.format(stage.workflow, stage)
        BODY = "\r\n".join(
            "From: %s" % FROM,
            "To: %s" % TO,
            "Subject: %s" % SUBJECT,
            "",
            text
        )
        server = smtplib.SMTP(HOST)
        server.sendmail(FROM, [TO], BODY)
        server.quit()

####################
# Workflow
####################

from cosmos.contrib.ezflow.dag import DAG, Apply, Split, Add
import tools

dag = ( DAG()
        | Add | [tools.ECHO(tags={'word': 'hello'}), tools.ECHO(tags={'word': 'world'})]
        | Apply | tools.FAIL # Automatically fail
)

#################
# Run Workflow
#################

dag.add_to_workflow(WF)
WF.run()