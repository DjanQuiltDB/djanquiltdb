from django.contrib import admin
from django.urls import include, path

# The admin URLs with the shard switcher URLs, in the order that the docs of the admin shard switcher give.
urlpatterns = [
    path('admin/', include('djanquiltdb.contrib.quilt_admin.urls')),
    path('admin/', admin.site.urls),
]
