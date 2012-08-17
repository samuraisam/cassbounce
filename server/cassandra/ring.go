package cassandra

/*

some notes:

	HERP:

	how astynax works:

		// operation has a partitioner
		Token tok = op.getToken(partitioner) // partitioner==randompartitioner = md5(bytesOfKey)

		// topology knows about all partition pools
		// it does a binary search on a sorted list of all pools 
		HostConnectionPoolPartition part = topology.getPartition(tok)

		// HostConnectionPoolPartition then knows about all hosts in general and keeps a mapping
		// of string(hostStr) => Host(host)


	POOL MANAGER

	NewPoolManager(
		// keeps looking at the ring, calls addHost/removeHost its listeners
		NewHostService()
		// maintains a consistent look at the topology (what tokens are where)
		NewTopology()
		// provides a way to get a token from a key
		NewRandomPartitioner()
	)

	Command.getToken(partitioner Partitioner, topology Topology)

	------

	POOLS!
		+ is initialized with a "ring" of hosts
			which is built by the HostService.initialize() which sniffs for all hosts
			-> HostService.sniffRing() - it calls Cassandra.describeRing() to get the list
			-> each result is added to self.addHost()
		+ has Topology which is a utility to b able to tell
		+ has HostService which monitors hosts
		implements "Host list listener" - HostService calls
			-> addHost(Host)
			-> removeHost(Host)
		when hosts are added or removed - it calls:
			1. create a pool for the host - add it to its map of pools
			2. add that pool to its Topology


	1. tok := Command.getToken() - 
		uses partitioner.getPartition(Command.KeyBytes())
		-> partitioner is some sort of implemention of org.apache.cassandra.dht.RandomPartitioner
		-> returns a Token obj
	2. part := Topology.getPartition(tok)
		-> before: Topology.setPools(listOfPools), where listOfPools is a list of HostConnectionPool objects
		-> 

*/