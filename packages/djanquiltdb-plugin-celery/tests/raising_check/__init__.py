"""
An app with a system check that raises when the router places the sharded Widget on the node 'default'. The router
places the Widget there only while DjanQuiltDB runs the system checks, because the router then places the sharded models
on every node. Only the settings in config.worker_check_settings install this app.
"""
