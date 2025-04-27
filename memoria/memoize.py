import functools
from .BasicCache import BasicCache


def memoize(cache, exclude_kwargs=None):
	"""
	makes a decorator for memoizing a function
	:type cache: BasicCache
	:type memory: bool
	:type read_files: bool
	:type capacity: Capacity or int or float or NoneType
	:type n_jobs: int or NoneType
	:type exclude_kwargs: list or tuple
	:type s3: amazonian.S3 or NoneType
	:type spark: pyspark.sql.session.SparkSession or NoneType
	:type echo: int or bool
	:type main_key: str or tuple or NoneType
	:rtype: callable
	"""
	if isinstance(exclude_kwargs, str):
		exclude_kwargs = [exclude_kwargs]

	def decorator(function):
		"""
		memoizes a function
		:type function: callable
		:rtype: callable
		"""
		child_cache = cache.children[str(function.__name__)]

		if not isinstance(exclude_kwargs, (list, tuple)) and exclude_kwargs is not None:
			raise TypeError(f'exclude_kwargs is of type {type(exclude_kwargs)}! None, list, and tuple are accepted.')

		@functools.wraps(function)
		def wrapper(*args, **kwargs):
			if exclude_kwargs is not None:
				kwargs_in_key = {key: value for key, value in kwargs.items() if key not in exclude_kwargs}
			else:
				kwargs_in_key = kwargs.copy()

			key = (function.__name__, function.__doc__, args, kwargs_in_key)
			if key in child_cache:
				result = child_cache[key]
			else:
				result = function(*args, **kwargs)
				child_cache[key] = result
			return result

		wrapper.parent_cache = cache
		wrapper.cache = child_cache
		wrapper.excluded_kwargs = exclude_kwargs
		return wrapper

	return decorator
