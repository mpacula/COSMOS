from django.contrib import admin
from models import Batch,Node,Workflow



class NodeInline(admin.TabularInline):
    model = Node


class BatchAdmin(admin.ModelAdmin):
    pass
#        inlines = [
#            NodeInline
#        ]
admin.site.register(Batch, BatchAdmin)




class WorkflowAdmin(admin.ModelAdmin):
    pass
admin.site.register(Workflow, WorkflowAdmin)

admin.site.register(Node)