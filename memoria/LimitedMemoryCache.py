from sys import getsizeof
from .MemoryCache import MemoryCache
import re

def get_number(s):
	try:
		return re.findall(r'^\d+(?:\.\d+)?', s)[0]
	except IndexError:
		raise ValueError(f'"{s}" is not a number!')


UNITS = {
	'byte': 1,
	'bytes': 1,
	'kb': 1024,
	'mb': 1024 ** 2,
	'gb': 1024 ** 3,
	'tb': 1024 ** 4,
	'pb': 1024 ** 5
}

class Capacity:
	def __init__(self, value):
		self._value = None
		self._used = 0

		self.value = value

	@property
	def value(self):
		return self._value

	@value.setter
	def value(self, value):
		if isinstance(value, str):
			number_str = get_number(value)
			the_rest = value[len(number_str):]
			if len(the_rest) > 0:
				unit = the_rest.lower().strip()
			else:
				unit = 'byte'

			number = float(number_str)
		else:
			number = value
			unit = 'byte'

		unit_size = UNITS[unit]
		self._value = number * unit_size
		self._unit = unit

	def size_in_unit(self):
		unit_size = UNITS[self._unit]
		return self._value / unit_size

	def usage_in_unit(self):
		unit_size = UNITS[self._unit]
		return self._used / unit_size

	def __str__(self):
		return f'{round(self.usage_in_unit(), 1)} / {round(self.size_in_unit(), 1)}{self._unit}'

	def __repr__(self):
		return f'Capacity: {round(self.size_in_unit(), 1)}{self._unit} - Used: {round(self.usage_in_unit(), 1)}{self._unit}'

	def add_obj_size(self, *args):
		sizes = [getsizeof(obj) for obj in args]
		self._used += sum(sizes)

	def subtract_obj_size(self, *args):
		sizes = [getsizeof(obj) for obj in args]
		self._used -= sum(sizes)

	def is_above_capacity(self):
		return self._used > self._value


class LimitedMemoryCache(MemoryCache):
	def __init__(self, n_jobs=None, spark=None, capacity=None, echo=0, main_key=None):
		super().__init__(n_jobs=n_jobs, spark=spark, echo=echo, main_key=main_key)
		self._capacity = None
		self.capacity = capacity

	def __str__(self):
		return str(self.capacity)

	def __repr__(self):
		return f'LimitedMemoryCache: {str(self.capacity)}'

	@property
	def capacity(self):
		return self._capacity

	@capacity.setter
	def capacity(self, value):
		if isinstance(value, Capacity):
			self._capacity = value
		elif value is None:
			self._capacity = None
		else:
			self._capacity = Capacity(value=value)

		if value is not None:
			self.adjust_size()

	def get_oldest(self):
		return next(iter(self._items))

	def _delete_oldest(self):
		oldest = self.get_oldest()
		objs = oldest, self._items[oldest]
		del self._items[oldest]
		self.capacity.subtract_obj_size(*objs)

	def adjust_size(self):
		if not self.capacity.is_above_capacity():
			return 0

		while self.capacity.is_above_capacity():
			self._delete_oldest()
		return 1

	def _set_item_with_hash_key(self, hash_key, value):
		# size should increase
		removing_objs = []
		adding_objs = [value]

		if hash_key in self._items:
			removing_objs.append(self._items[hash_key])
			# but if there is already something there, the size of the old item should be deducted
			del self._items[hash_key]
			if self.capacity is not None:
				self.capacity.subtract_obj_size(*removing_objs)
		else:
			adding_objs.append(hash_key)

		super()._set_item_with_hash_key(hash_key=hash_key, value=value)
		if self.capacity is not None:
			self.capacity.add_obj_size(*adding_objs)
			return self.adjust_size()
		else:
			return 0

	def set_item(self, item, value):
		hash = self.get_hash(item=item)
		return self._set_item_with_hash_key(hash_key=hash.key, value=value)

	def get_item(self, item):
		hash = self.get_hash(item)
		value = super().get_item(item=hash)
		# remove and put back to make the item the last in the dictionary
		del self._items[hash.key]
		self._items[hash.key] = value
		return value

	def delete_item(self, item):
		hash = self.get_hash(item)
		removing_obj = [hash.key, self._items[hash.key]]
		super().delete_item(hash)
		if self.capacity is not None:
			self.capacity.subtract_obj_size(*removing_obj)
