from django.conf.urls.defaults import patterns, include, url
import Workflow
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

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
    url(r'JobManager/JobAttempt/(\d+)/$', 'JobManager.views.jobAttempt',name='jobAttempt_view'),
    url(r'JobManager/JobAttempt/(\d+)/output/(.+)$', 'JobManager.views.jobAttempt_output',name='jobAttempt_output'),
    url(r'SGE/$', 'Cosmos.views.SGE',name='sge'),
    url(r'LSF/$', 'Cosmos.views.LSF',name='lsf'),
    url(r'Workflow/$', 'Workflow.views.index',name='workflow'),
    url(r'Workflow/(\d+)/$', 'Workflow.views.view',name='workflow_view'),
    url(r'Workflow/view_log/(\d+)/$', 'Workflow.views.view_log',name='workflow_view_log'),
    url(r'Workflow/analysis/(\d+)/$', 'Workflow.views.analysis',name='workflow_analysis'),
    url(r'Workflow/Batch/(\d+)/$', 'Workflow.views.batch_view',name='batch_view'),
    url(r'Workflow/Node/(\d+)/$', 'Workflow.views.node_view',name='node_view'),
    url(r'^$', 'Cosmos.views.index',name='home'),
)
from django.conf import settings
urlpatterns += staticfiles_urlpatterns()

urlpatterns += patterns('',
     url(r'^media/(?P<path>.*)$', 'django.views.static.serve', {
         'document_root': settings.MEDIA_ROOT,
     }),
)