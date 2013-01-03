from django.conf.urls.defaults import patterns, include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

#from dajaxice.core import dajaxice_autodiscover, dajaxice_config
#dajaxice_autodiscover()

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'Cosmos.views.home', name='home'),
    # url(r'^Cosmos/', include('Cosmos.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
    #url(dajaxice_config.dajaxice_url, include('dajaxice.urls')),
    url(r'JobManager/JobAttempt/(\d+)/$', 'JobManager.views.jobAttempt',name='jobAttempt_view'),
#    url(r'JobManager/JobAttempt/(\d+)/output/(.+)$', 'JobManager.views.jobAttempt_output',name='jobAttempt_output'),
    url(r'Workflow/TaskFile/(\d+)/$', 'Workflow.views.taskfile_view',name='taskfile_view'),
    url(r'JobManager/JobAttempt/(\d+)/profile_output/$', 'JobManager.views.jobAttempt_profile_output',name='jobAttempt_profile_output'),
    url(r'GridEngine/$', 'Cosmos.views.GridEngine',name='GridEngine'),
    url(r'LSF/$', 'Cosmos.views.LSF',name='lsf'),
    url(r'Workflow/$', 'Workflow.views.index',name='workflow'),
    url(r'Workflow/(\d+)/view_log/$', 'Workflow.views.view_log',name='workflow_view_log'),
    url(r'Workflow/(\d+)/analysis/$', 'Workflow.views.analysis',name='workflow_analysis'),
    url(r'Workflow/(\d+)/visualize/$', 'Workflow.views.visualize',name='workflow_visualize'),
    url(r'Workflow/(\d+)/visualize/as_img/$', 'Workflow.views.visualize_as_img',name='visualize_as_img'),
    url(r'Workflow/(\d+)/$', 'Workflow.views.view',name='workflow_view'),
    url(r'Workflow/(\d+)/stage_table/$', 'Workflow.views.workflow_stage_table',name='workflow_stage_table'),
    url(r'Workflow/Stage/(\d+)/table/$', 'Workflow.views.stage_table',name='stage_table'),
    url(r'Workflow/Stage/(\d+)/stage_task_table/$', 'Workflow.views.stage_task_table',name='stage_task_table'),
    url(r'Workflow/Stage/(\d+)/$', 'Workflow.views.stage_view',name='stage_view'),
    url(r'Workflow/Task/(\d+)/$', 'Workflow.views.task_view',name='task_view'),
    url(r'^$', 'Cosmos.views.index',name='home'),
)
from django.conf import settings
urlpatterns += staticfiles_urlpatterns()

urlpatterns += patterns('',
     url(r'^media/(?P<path>.*)$', 'django.views.static.serve', {
         'document_root': settings.MEDIA_ROOT,
     }),
)