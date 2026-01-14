from django.urls import path

from djanquiltdb.contrib.quilt_admin.views import SwitchShardView

urlpatterns = [
    path('switch-shard/', SwitchShardView.as_view(), name='djanquiltdb_switch_shard'),
]
