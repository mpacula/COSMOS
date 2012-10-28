from django import forms
class NodeFilterForm(forms.Form):
    type = forms.HiddenInput() #attribute or tag
    key = forms.HiddenInput()
    value = forms.ChoiceField()