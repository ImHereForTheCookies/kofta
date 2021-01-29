import pickle
from functools import wraps


def save_offsets(func):
    """
    Decorator to wrap a function from a class that consumes from a kafka topic and handles when an exception occurs that
    causes the stream to be halted. This allows for functions to avoid re-reading topics from the beginning on large reads.
    Args:
        func: function being decorated. Only useful for functions that consume from a kafka topic and inherit from Consumer

    Returns: Saves offset locations

    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            print(e)
            save_location = kwargs.get('save_location', None)
            print(f'Saving partition offsets at {save_location}')
            with open(save_location, 'wb') as f:
                pickle.dump(self.partitions, f)

    return wrapper


def load_offsets(func):
    """

    Args:
        func:

    Returns:

    """