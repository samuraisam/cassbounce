import time
import pycassa


pool = pycassa.ConnectionPool('cassbounce', timeout=.1, pool_size=1, max_retries=0, server_list=['0.0.0.0:9666'], credentials={'username': 'fart', 'password': 'fart'})
# pool = pycassa.ConnectionPool('cassbounce', server_list=['0.0.0.0:9160'])
pool.fill()

cf = pycassa.ColumnFamily(pool, 'farts')

print list(cf.get_range())

while True:
	rowkey = raw_input("key: ")
	column = raw_input("col: ")
	value = raw_input("value: ")

	cf.insert(rowkey, {column: value})
