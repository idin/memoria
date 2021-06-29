import hashlib
import base64
import base32hex


def hash_file(path, base=64, block_size=65536):
	hash_maker = hashlib.sha256()
	with open(path, 'rb') as file:
		block = file.read(block_size)
		while len(block) > 0:
			hash_maker.update(block)
			block = file.read(block_size)
	if base == 64:
		return base64.b64encode(hash_maker.digest()).decode()
	elif base == 32:
		return base32hex.b32encode(hash_maker.digest()).replace('=', '-')
	else:
		raise ValueError(f'base{base} is unknown!')
