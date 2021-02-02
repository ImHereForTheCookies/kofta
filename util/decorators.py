import pickle
from functools import wraps

___all__ = ["offset_storage"]


def offset_storage(func):
    """
    Decorator to wrap a function from a class that consumes from a kafka topic and handles when an exception occurs that
    causes the stream to be halted. This allows for functions to avoid re-reading topics from the beginning on large reads.
    Args:
        func: function being decorated. Only useful for functions that consume from a kafka topic and inherit from Consumer

    Returns: Saves offset locations

    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.file_path is None:
            print("No file path specified for saving or loading offsets.\n"
                  "\tUse offsets_file in __init__ to use offset recovery.\n"
                  "\tUse load in __init__ to recover offsets from offsets_file path")
            if kwargs.get("load", False):
                with open(self.file_path, 'rb') as f:
                    self.partitions = pickle.load(f)
        elif not kwargs.get("load", False):
            print("Use load in __init__ to recover offsets from offsets_file path")
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            print(e)
            if self.file_path is not None:
                print(f'Saving partition offsets at {self.file_path}')
                with open(self.file_path, 'wb') as f:
                    pickle.dump(self.partitions, f)

    return wrapper


# def load_offsets(func):
#     """
#
#     Args:
#         func:
#
#     Returns:
#
#     """
#     @wraps(func)
#     def wrapper(self, *args, **kwargs):
#         if self.file_path is None and kwargs.get("load", False):
#             print("No file path specified for saving of loading offsets. "
#                   "Use offsets_file in __init__ to use this feature.")
#         else:
#             self.partitions = pickle.load(self.file_path)
#         return func(self, *args, **kwargs)
#
#     return wrapper
