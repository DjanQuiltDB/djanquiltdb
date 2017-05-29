def mirror():
    """
    A decorator for marking a model as being mirror across the various nodes.
    """
    def configure(cls):
        return cls

    return configure


def shard():
    """
    A decorator for marking a model as being sharded.
    """
    def configure(cls):
        return cls

    return configure
