"""URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
"""

from django.contrib.auth import views as auth_views
from django.urls import re_path
from example.views import HomeView

urlpatterns = [
    re_path(r'^$', HomeView.as_view(), name='home'),
    re_path(r'^login/$', auth_views.LoginView.as_view(template_name='users/login.html'), name='login'),
]
