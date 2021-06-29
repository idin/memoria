import codecs


def to_bytes(s):
	"""
	If you don't know if a stringlike object is a Python 2 string (bytes) or Python 3 string (unicode).
	You could have a generic converter.
	Source: https://stackoverflow.com/questions/60368956/attributeerrorbytes-object-has-no-attribute-encode
	:type s: str or bytes
	:rtype: bytes
	"""
	if type(s) is bytes:
		return s
	elif type(s) is str:
		return codecs.encode(s, 'utf-8')
	else:
		raise TypeError(f"Expected bytes or string, but got {type(s)}")
