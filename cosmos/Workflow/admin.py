from django.contrib import admin
from models import Stage,Task,Workflow



class TaskInline(admin.TabularInline):
    model = Task


class StageAdmin(admin.ModelAdmin):
    pass
#        inlines = [
#            TaskInline
#        ]
admin.site.register(Stage, StageAdmin)




class WorkflowAdmin(admin.ModelAdmin):
    pass
admin.site.register(Workflow, WorkflowAdmin)

admin.site.register(Task)