from disk import Path
from .BasicCache import BasicCache, Hash


class DiskCache(BasicCache):
	def __init__(self, path, n_jobs=None, s3=None, spark=None, echo=0, main_key=None):
		"""
		:type path: amazonian.S3Path or disk.Path or str
		:type spark: pyspark.sql.session.SparkSession
		"""
		super().__init__(n_jobs=n_jobs, spark=spark, echo=echo, main_key=main_key)
		self._path = None
		if s3 is None:
			self.path = path
		elif isinstance(path, str):
			self.path = s3 / path

		self._spark = spark

	def __str__(self):
		return f'{self.path.path}'

	def __repr__(self):
		return f'DiskCache at {self.path.path}'

	@property
	def path(self):
		"""
		:rtype: disk.Path or amazonian.S3Path
		"""
		return self._path

	@path.setter
	def path(self, value):
		if isinstance(value, str):
			if value.startswith('s3://'):
				raise ValueError('s3 paths should be provided as a S3Path object from the amazonian library.')
			else:
				path = Path(value)
		else:
			path = value

		if not path.exists():
			path.make_dir()
		elif path.is_file():
			raise FileExistsError(f'path {path.path} is a file!')

		if self._spark is not None:
			try:
				path.set_spark(self._spark)
			except AttributeError:
				pass
		else:
			try:
				self._spark = path.spark
			except AttributeError:
				pass

		self._path = path

	def get_item_path(self, item):
		"""
		:type key: str or Hash
		:rtype: Path or amazonian.S3.S3Path
		"""
		hash = self.get_hash(item=item)
		return self._path / f'cache_{hash.key}'

	def contains(self, item):
		hash = self.get_hash(item=item)
		path = self.get_item_path(hash)
		if path.exists():
			return True
		elif (path + '.pickle').exists():
			return True
		elif (path + '.parquet').exists():
			return True
		else:
			return False

	def set_item(self, item, value):
		hash = self.get_hash(item)
		path = self.get_item_path(hash)
		result = path.save(obj=value, mode='wb')
		if self._echo:
			print(f'{result} saved!')
		return result

	def get_item(self, item):
		hash = self.get_hash(item)
		path = self.get_item_path(hash)
		if path.exists():
			if self._echo:
				print(f'"{item}" exists at "{path.path}"')
			value = path.load(spark=self._spark)

		else:
			pickle_path = path + '.pickle'
			if pickle_path.exists():
				if self._echo:
					print(f'{pickle_path.path} loaded!')
				value = pickle_path.load(spark=self._spark)
			else:
				parquet_path = path + '.parquet'
				if parquet_path.exists():
					if self._echo:
						print(f'{parquet_path.path} loaded!')
					value = parquet_path.load(spark=self._spark)
				else:
					raise KeyError(f'"{item}" does not exist!')

		return value

	def delete_item(self, item):
		hash = self.get_hash(item)
		path = self.get_item_path(hash)
		if path.exists():
			path.delete()
		else:
			raise FileNotFoundError(f'"{item}" not found!')

	def create_child(self, name):

