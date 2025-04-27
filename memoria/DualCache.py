import functools
from .DiskCache import DiskCache
from .LimitedMemoryCache import LimitedMemoryCache, Capacity
from .BasicCache import Hash, BasicCache, CacheChildren
from .memoize import memoize


class DualCache(BasicCache):
	def __init__(
			self, path=None, memory=True, read_files=True, capacity=None, n_jobs=None,
			s3=None, spark=None, echo=0, main_key=None
	):
		"""

		:type path: amazonian.S3Path or disk.Path or str
		:type memory: bool
		:type read_files: bool
		:type capacity: Capacity or int or float or NoneType
		:type n_jobs: int or NoneType
		:type s3: amazonian.S3
		:type spark: pyspark.sql.session.SparkSession
		:type echo: int or bool
		:type main_key: str or tuple or NoneType
		"""
		super().__init__(n_jobs=n_jobs, spark=spark, echo=echo, main_key=main_key)
		if not memory and path is None:
			raise ValueError('either memory should be True or path should be provided!')

		if memory:
			self._memory_cache = LimitedMemoryCache(
				capacity=capacity, n_jobs=n_jobs, spark=spark, echo=echo, main_key=main_key
			)
		else:
			self._memory_cache = None

		if path is None:
			self._disk_cache = None
		else:
			self._disk_cache = DiskCache(path=path, n_jobs=n_jobs, s3=s3, spark=spark, echo=echo, main_key=main_key)

		if read_files:
			self.read_files()

	def __str__(self):
		parts = []
		if self._memory_cache is not None:
			parts.append(f'Memory: {str(self._memory_cache)}')

		if self._disk_cache is not None:
			parts.append(f'Disk: {str(self._disk_cache)}')

		return ' - '.join(parts)

	def __repr__(self):
		return f'DualCache [{str(self)}]'

	def get_capacity(self):
		try:
			return self._memory_cache.capacity
		except AttributeError:
			return None

	def create_child(self, name):
		child_main_key = self._get_child_main_key(name=name)
		child_path = self._get_child_path(name=name)
		capacity = self.get_capacity()

		child = DualCache(
			path=child_path,
			memory=self._memory_cache is not None,
			read_files=False,
			capacity=capacity,
			n_jobs=self._n_jobs,
			spark=self._spark,
			echo=self._echo,
			main_key=child_main_key
		)
		if self._memory_cache is not None:
			child.set_items(items=self._items)
		return child

	@property
	def _items(self):
		return self._memory_cache._items

	@_items.setter
	def _items(self, value):
		self._memory_cache._items = value

	@property
	def path(self):
		"""
		:rtype: disk.Path or amazonian.S3Path
		"""
		if self._disk_cache is None:
			return None
		else:
			return self._disk_cache.path

	@property
	def capacity(self):
		"""
		:rtype: Capacity
		"""
		if self._memory_cache is None:
			raise RuntimeError('No memory_cache!')
		else:
			return self._memory_cache.capacity

	def read_files(self):
		if self._memory_cache is None:
			return 0

		if self._memory_cache._capacity is None:
			return 0

		if self.capacity.is_above_capacity():
			return 1

		for sub_path in self.path.ls():
			if self.capacity.is_above_capacity():
				return 1
			if sub_path.name.startswith('cache_'):
				item = sub_path.name[6:]
				if sub_path.is_file():
					value = sub_path.load()
					hash_key = item
					size_was_adjusted = self._memory_cache._set_item_with_hash_key(hash_key=hash_key, value=value)
					if size_was_adjusted == 1:
						return 1
				elif sub_path.is_empty():
					sub_path.delete()

				else:
					child = self.children[item]
					size_was_adjusted = child.read_files()
					if size_was_adjusted == 1:
						return 1
		return 0

	def get_hash(self, item):
		return Hash(item, n_jobs=self._n_jobs, main_key=self._main_key)

	def contains(self, item):
		hash = self.get_hash(item)
		if self._memory_cache is not None:
			if self._memory_cache.contains(hash):
				return True
		if self._disk_cache is None:
			raise RuntimeError('No disk_cache!')
		return self._disk_cache.contains(hash)

	def get_item(self, item):
		hash = self.get_hash(item)
		try:
			return self._memory_cache.get_item(hash)
		except (KeyError, AttributeError) as e:
			if self._disk_cache is None:
				raise e
			value = self._disk_cache.get_item(hash)
			if self._memory_cache is not None:
				self._memory_cache.set_item(item=hash, value=value)
			return value

	def set_item(self, item, value):
		hash = self.get_hash(item)
		if self._memory_cache is not None:
			self._memory_cache.set_item(item=hash, value=value)
		if self._disk_cache is not None:
			self._disk_cache.set_item(item=hash, value=value)

	def delete_item(self, item):
		hash = self.get_hash(item)
		try:
			self._memory_cache.delete_item(item=hash)
		except (KeyError, AttributeError) as e:
			if self._disk_cache is None:
				raise e
			self._disk_cache.delete_item(hash)
		else:
			try:
				if self._disk_cache is not None:
					self._disk_cache.delete_item(hash)
			except KeyError:
				pass

	def memoize(self, exclude_kwargs=None):
		"""
		makes a decorator for memoizing a function
		:type exclude_kwargs: callable
		:rtype: callable
		"""
		return memoize(cache=self, exclude_kwargs=exclude_kwargs)
