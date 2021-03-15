import pickle
from functools import wraps

___all__ = ["offset_storage"]


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
        if not kwargs.get('save_path', False):
            print("No file path specified for saving offsets.\n"
                  "\tUse save_path in function argument to save offsets when interrupted.\n")
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            print(e)
            if kwargs.get('save_path', False):
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
