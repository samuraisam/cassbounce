import time
import pycassa
import uuid


pool = pycassa.ConnectionPool('cassbounce', timeout=.1, pool_size=1, max_retries=0, server_list=['0.0.0.0:9666'], credentials={'username': 'fart', 'password': 'fart'})
# pool = pycassa.ConnectionPool('cassbounce', server_list=['0.0.0.0:9160'])
pool.fill()

cf = pycassa.ColumnFamily(pool, 'farts')

print list(cf.get_range())
k = uuid.uuid1().hex
cf.insert(k, {uuid.uuid1().hex: uuid.uuid4().hex})
print "get =>", cf.get(k)

# while True:
# 	rowkey = raw_input("key: ")
# 	column = raw_input("col: ")
# 	value = raw_input("value: ")

# 	cf.insert(rowkey, {column: value})

# 	print "get =>", cf.get(rowkey)
