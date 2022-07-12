from .BasicCache import BasicCache


class MemoryCache(BasicCache):
	def __init__(self, n_jobs=None, spark=None, echo=0, main_key=None):
		super().__init__(n_jobs=n_jobs, spark=spark, echo=echo, main_key=main_key)
		self._items = {}


	def contains(self, item):
		hash = self.get_hash(item)
		return hash.key in self._items

	def _set_item_with_hash_key(self, hash_key, value):
		self._items[hash_key] = value

	def set_item(self, item, value):
		if self._echo:
			print(f'setting {item}')
		hash = self.get_hash(item)
		self._set_item_with_hash_key(hash_key=hash.key, value=value)

	def get_item(self, item):
		if self._echo:
			print(f'getting {item}')
		hash = self.get_hash(item=item)
		try:
			return self._items[hash.key]
		except KeyError:
			raise KeyError(f'"{item}" not found!')

	def delete_item(self, item):
		hash = self.get_hash(item)
		if self._echo:
			print(f'deleting {item}')

		try:
			del self._items[hash.key]
		except KeyError:
			raise KeyError(f'"{item}" not found!')
