# 'key_to_keys' and 'keys_to_key' are functions originally defined in 'idx_bit_translate.py'
def key_to_keys(key):
    key_b = '{0:032b}'.format(key)
    return int(key_b[:16], 2), int(key_b[16:], 2)


def keys_to_key(key1, key2):
    key_b = '{0:016b}'.format(key1) + '{0:016b}'.format(key2)
    return int(key_b, 2)