from pyspark.sql import DataFrame as SparkDF
from pyspark.sql.types import Row as SparkRow
from pandas import DataFrame as PandasDF

from multiprocessing.pool import ThreadPool

import numpy as np
from functools import reduce
from pyspark.sql.types import LongType, StringType
from pyspark.sql import functions as f
import hashlib
import base64

from .to_bytes import to_bytes


def hash_int(x, n_jobs):
	s = represent_and_encode(x, n_jobs=n_jobs)
	_hash_maker = hashlib.sha256()

	encoded = to_bytes(s)  # instead of x.encode()
	_hash_maker.update(encoded)
	return hash(base64.b64encode(_hash_maker.digest()).decode())


def concat_spark_columns(data, name='concat_column', sep='|', return_data=False):
	"""
	concatenates each row and creates a new column
	:type data: SparkDF
	:type name: str
	:type sep: str
	:type return_data: bool
	:param return_data: if True, the whole data frome with the new column is returned, otherwise just the column
	:rtype: SparkDF
	"""

	@f.udf(StringType())
	def _to_string(x):
		if x is None:
			return 'NONE'
		if isinstance(x, SparkRow):
			x = x.asDict(recursive=True)
		return x if isinstance(x, str) else str(x).lower()

	data = data.withColumn(name, f.concat_ws(sep, *[_to_string(col) for col in data.columns]))

	if return_data:
		return data
	else:
		return data.select(name)


def map_spark_to_int(data, n_jobs):
	def _map_function(row):
		"""
		:type row: Row
		:rtype: list[int]
		"""
		return [hash_int(x, n_jobs=n_jobs) for column, x in row.asDict().items()]

	return data.rdd.map(_map_function).toDF(data.columns)


def aggregate_spark_int(data, n_jobs):
	data.cache()
	data_size = data.count()
	n_jobs = min(n_jobs, round(data_size ** 0.5))

	@f.udf(LongType())
	def _spark_bitwise_xor(_numbers):
		return reduce(lambda x, y: x ^ y, _numbers)

	def _aggregate_spark_int(_data):
		return _data.agg(*[
			_spark_bitwise_xor(f.collect_list(col)).alias(col)
			for col in _data.columns
		])

	if n_jobs == 1:
		return _aggregate_spark_int(data)

	else:
		def _union_all(_data_list):
			return reduce(SparkDF.unionAll, _data_list)

		data_list = data.randomSplit([1.0] * n_jobs)
		pool = ThreadPool(n_jobs)
		result_list = pool.map(_aggregate_spark_int, data_list)
		union = _union_all(result_list)

		return _aggregate_spark_int(union)


def concat_pandas_columns(data, name='concat_column', sep='|', return_data=False):
	"""
	concatenates each row and creates a new column
	:type data: PandasDF
	:type name: str
	:type sep: str
	:type return_data: bool
	:param return_data: if True, the whole dataframe with the new column is returned, otherwise just the column
	:rtype: PandasDF
	"""
	def concat_row(x):
		return sep.join([
			'NONE' if value is None else (value if isinstance(value, str) else str(value).lower())
			for value in x.values
		])

	data = data.copy()
	if name in data.columns:
		data.drop(columns=name, inplace=True)
	data[name] = data.apply(concat_row, axis=1)

	if return_data:
		return data
	else:
		return data[[name]]


def map_pandas_to_int(data, n_jobs):
	def _hash_int(x):
		return hash_int(x, n_jobs=n_jobs)

	try:
		pandas_int_data = data.applymap(_hash_int, na_action=None)
	except TypeError:
		pandas_int_data = data.apply(lambda x: x.map(_hash_int, na_action=None))
	return pandas_int_data


def aggregate_pandas_int(data):
	def pandas_bitwise_xor(numbers):
		return reduce(lambda x, y: np.bitwise_xor(x, y), numbers)

	return data.agg(pandas_bitwise_xor, axis='rows')


def represent_and_encode(obj, n_jobs):
	class_representation = f'{obj.__class__.__name__}'
	obj_representation = repr(make_hashable(obj, n_jobs=n_jobs))
	representation = f'{class_representation}|{obj_representation}'
	return representation.encode()


def make_hashable(obj, n_jobs):
	if hasattr(obj, '__hashkey__'):
		return make_hashable(obj.__hashkey__())
	else:
		if isinstance(obj, (tuple, list)):
			return tuple((make_hashable(e, n_jobs=n_jobs) for e in obj))

		if isinstance(obj, dict):
			return tuple(sorted((k, make_hashable(v, n_jobs=n_jobs)) for k, v in obj.items()))

		if isinstance(obj, (set, frozenset)):
			return tuple(sorted(make_hashable(e, n_jobs=n_jobs) for e in obj))

		if isinstance(obj, (SparkDF, PandasDF)):
			return make_dataframe_hashable(data=obj, n_jobs=n_jobs)

		return obj


def make_dataframe_hashable(data, n_jobs):
	"""
	:type data: SparkDF or PandasDF
	:type n_jobs: int
	:rtype: tuple
	"""

	# turn the dataframe into a single column of concatenated values
	# this makes sure if two cells in the same column are swapped the change is captured in this column
	# even though it will be ignored by the aggregation of each column
	data = data
	name = 'concat_column'
	sep = '|'
	return_data = False

	if isinstance(data, SparkDF):
		spark_compressed = concat_spark_columns(data=data, name=name, sep=sep, return_data=return_data)
		use = 'spark'
	elif isinstance(data, PandasDF):
		pandas_compressed = concat_pandas_columns(data=data, name=name, sep=sep, return_data=return_data)
		use = 'pandas'
	else:
		raise TypeError(f'data of type "{type(data)}" is not supported!')

	# convert each value in the column into an int hash

	if use == 'spark':
		spark_int_data = map_spark_to_int(spark_compressed, n_jobs=n_jobs)
		spark_aggregate = aggregate_spark_int(spark_int_data, n_jobs=n_jobs)
		aggregate_dictionary = spark_aggregate.first().asDict()

	elif use == 'pandas':
		pandas_int_data = map_pandas_to_int(pandas_compressed, n_jobs=n_jobs)
		pandas_aggregate = aggregate_pandas_int(pandas_int_data)
		aggregate_dictionary = pandas_aggregate.to_dict()

	else:
		raise RuntimeError(f'Usage "{use}" is not supported!')

	return tuple(sorted((k, v) for k, v in aggregate_dictionary.items()))
