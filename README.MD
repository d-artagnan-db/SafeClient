# SafeClient

This component is responsible for abstracting from client applications the multiple underlying databases and protecting plaintext requests in to secure requests.


#Dependencies

- ```cryptoenv``` ([High-level Wrapper for protocols](https://github.com/d-artagnan-db/CryptoEnv.git))
- ```smpc``` ([Secure Multi-Party Computation Library](https://github.com/d-artagnan-db/SMPC.git)
- ```testingutils``` ([Utility Testing component](https://github.com/d-artagnan-db/TestingUtils.git))
- ```hbaseInterfaces``` ([Database Interfaces](https://github.com/d-artagnan-db/HBaseInterfaces.git))
- ```protocommunications``` ([Protocol Buffers component](https://github.com/d-artagnan-db/ProtoCom.git))
- ```safemapper``` ([Secure Schema Mapping module](https://github.com/d-artagnan-db/SafeMapper.git))
- ```saferegions``` ([d'Artagnan SaveServer](https://github.com/d-artagnan-db/SafeServer.git))


# How to use Install SafeClient?

mvn install -DskipTests

# How to use?

After isntalled and deployed, SafeClient can be used as a standard HBase Client without requiring almost no modificaiton.
The only modification is the addition of new configuation on the hbase-site. The client must specify the multiple clouds IPs
and Ports as well as the configuration file that specify how each HTable column must be protected. An example of a deployment 
configuration can be found in the resources folder in the hbase-client.xml file. 

# Building a Schema file

In order to SafeClient correctly protects sensitive data, users must specify a secure schema file. Such file contains the 
essential information about the database schema, as well as the security properties that one wants to achieve. 
Some schema files can be found in *resources/schemas.xml*.

