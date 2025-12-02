from functools import wraps
from timeit import default_timer as timer

def time_it(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = timer()
        result = func(*args, **kwargs)
        end = timer()
        print(f"Time taken: {end - start}")
        return result
    return wrapper