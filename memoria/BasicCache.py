from .hash_object import smart_hash
from ._get_number_of_cores import get_number_of_jobs


class Hash:
	def __init__(self, item, n_jobs, main_key):
		if isinstance(item, Hash):
			self._key = item.key
		else:
			if main_key is not None:
				item = (main_key, item)
			self._key = smart_hash(item, n_jobs=n_jobs, base=32)

	@property
	def key(self):
		"""
		:rtype: str
		"""
		return self._key

	def __getstate__(self):
		return self._key

	def __setstate__(self, state):
		self._key = state


class CacheChildren:
	def __init__(self, parent):
		"""
		:type parent: DualCache
		"""
		self._parent = parent
		self._dictionary = {}

	def __getitem__(self, item):
		"""
		:type item: str
		:rtype: DualCache
		"""
		if item not in self._dictionary:
			self._dictionary[item] = self._parent.create_child(name=item)

		return self._dictionary[item]


class BasicCache:
	def __init__(self, n_jobs, spark, echo, main_key):
		if n_jobs is None:
			n_jobs = get_number_of_jobs()
		self._n_jobs = n_jobs
		self._spark = spark
		self._echo = echo
		self._main_key = main_key
		self._children = CacheChildren(parent=self)

	def get_hash(self, item):
		return Hash(item=item, n_jobs=self._n_jobs, main_key=self._main_key)

	def __setitem__(self, item, value):
		return self.set_item(item=item, value=value)

	def __getitem__(self, item):
		return self.get_item(item)

	def __contains__(self, item):
		return self.contains(item)

	def __delitem__(self, item):
		return self.delete_item(item)

	def contains(self, item):
		raise NotImplementedError('contains not implemented for BasicCache')

	def set_item(self, item, value):
		raise NotImplementedError('set_item not implemented for BasicCache')

	def get_item(self, item):
		raise NotImplementedError('get_item not implemented for BasicCache')

	def delete_item(self, item):
		raise NotImplementedError('delete_item not implemented for BasicCache')

	@property
	def children(self):
		"""
		:rtype: CacheChildren
		"""
		return self._children

	def _get_child_main_key(self, name):
		if self._main_key is None:
			child_main_key = name
		elif isinstance(self._main_key, tuple):
			child_main_key = (*self._main_key, name)
		else:
			child_main_key = (self._main_key, name)
		return child_main_key

	def _get_child_path(self, name):
		try:
			path = self.path
		except AttributeError:
			path = None

		if path is None:
			child_path = None
		else:
			child_path = self.path / f'cache_{name}'
		return child_path

	def get_capacity(self):
		try:
			return self._capacity
		except AttributeError:
			return None

	def get_items(self):
		try:
			return self._items
		except AttributeError:
			return None

	def set_items(self, items):
		if hasattr(self, '_items'):
			self._items = items

	def create_child(self, name):
		raise NotImplementedError(f'create_child is not implemented for class {self.__class__.__name__}')
