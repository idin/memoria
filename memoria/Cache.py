from disk import Path
from .hash_object import smart_hash
from ._get_number_of_cores import get_number_of_jobs


class Cache:
	def __init__(self, path, n_jobs=None, spark=None, echo=1):
		"""
		:type path: amazonian.S3.S3Path or Path or str
		:type spark: pyspark.sql.session.SparkSession
		"""
		if n_jobs is None:
			n_jobs = get_number_of_jobs()

		if isinstance(path, str):
			path = Path(path)

		self._path = path
		self._spark = spark
		self._items = set()
		self._echo = echo
		self._n_jobs = n_jobs

	def get_key(self, item):
		return smart_hash(item, n_jobs=self._n_jobs, base=32)

	def get_path(self, key):
		"""
		:type key: str
		:rtype: amazonian.S3.S3Path or Path
		"""
		return self._path / key

	def set_item(self, item, value):
		key = self.get_key(item)
		path = self.get_path(key)
		result = path.save(obj=value, mode='overwrite')
		if self._echo:
			print(f'{result} saved!')
		self._items.add(key)
		return result

	def get_item(self, item):
		key = self.get_key(item)
		path = self.get_path(key)
		if path.exists():
			if self._echo:
				print(f'{path.path} loaded!')
			self._items.add(key)
			return path.load(spark=self._spark)
		else:
			pickle_path = path + '.pickle'
			if pickle_path.exists():
				if self._echo:
					print(f'{pickle_path.path} loaded!')
				self._items.add(key)
				return pickle_path.load()
			else:
				parquet_path = path + '.parquet'
				if parquet_path.exists():
					if self._echo:
						print(f'{parquet_path.path} loaded!')
					self._items.add(key)
					return parquet_path.load(spark=self._spark)
				else:
					raise KeyError(f'key {key} does not exist!')

	def contains(self, item):
		key = self.get_key(item)
		if key in self._items:
			return True
		else:
			path = self.get_path(key)
			if path.exists():
				self._items.add(key)
				return True
			elif (path + '.pickle').exists():
				return True
			elif (path + '.parquet').exists():
				return True
			else:
				return False

	def delete_item(self, item):
		key = self.get_key(item)
		if key in self._items:
			self._items.remove(key)
			path = self.get_path(key)
			if path.exists():
				path.delete()
			else:
				raise FileNotFoundError(f'key "{key}" not found!')
		else:
			path = self.get_path(key)
			if path.exists():
				path.delete()
			else:
				raise KeyError(f'key "{key}" not found and path "{path}" not found!')

	def __setitem__(self, item, value):
		return self.set_item(item=item, value=value)

	def __getitem__(self, item):
		return self.get_item(item)

	def __contains__(self, item):
		return self.contains(item)

	def __delitem__(self, item):
		return self.delete_item(item)
