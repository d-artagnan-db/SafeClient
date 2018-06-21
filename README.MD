# SafeClient

Safeclient is SafeCloud's client application and entrypoint for any application that uses SafeCloud.
This component is based on HBase 0.98 and exposes an *Extended HTable* API defined in the project hbaseInterfaces. An
extended interface of HTableInterface API was necessary as it doest not define every method available in HTable and DQE
requires some of these methods.

# Supported Mods

SafeClient has t

- SafeNoSQL
- d'Artagnan



# How to use SafeNoSQL?

TODO



# Deploying SafeNoSQL
TODO

## From sources
SafeNoSQL relies on several projects:

- ```std-enc``` ([Standard Encryption CryptoBox](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/std-enc))
- ```det-enc``` ([Deterministic Encryption CryptoBox](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/det-enc))
- ```ope-enc``` ([Order-Preserving Encryption CryptoBox](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/ope-enc))
- ```fpe-enc``` ([Format-Preserving Encryption CryptoBox](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/fpe-enc))
- ```cryptoenv``` ([Wrapper of all CryptoBoxes](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/CryptoEnv))
- ```smpc``` ([Secure Multi-Party Computation module](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/smpc))
- ```testingutils``` ([Utility Testing module](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/testingutils))
- ```hbaseInterfaces``` ([Database Interfaces](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/hbaseInterfaces))
- ```protocommunications``` ([Protocol Buffers module](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/protocommunication))
- ```safemapper``` ([Secure Schema Mapping module](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/safemapper))
- ```saferegions``` ([d'Artagnan module](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/saferegions))
- ```safeclient``` ([actual SafeNoSQL module](https://gitlab.lsd.di.uminho.pt/SafeCloud-eu/safeclient))

All projects are built with maven. Therefore, to install each module, go to the source of the project and run:

    mvn package

    mvn install

For the CryptoBox projects (std-enc, det-enc and ope-enc), since there core is built in C, first run the install.sh script 
and then run it as a regular maven project.

Use -DskipTests if you want to skip the Unit Tests verification.

# Configuration file
Similarly to a vanilla database client, SafeNoSQL requires a simple configuration file (.xml) that comprises the essential 
information to setup the database client. For instance, HBase-/DynamoDB-based configuration can be defined in this file. 
In addition to the original configurations, SafeNoSQL supports three new configurations to enable the security on the database. 

- **baseTable**: type of table type to use. *HTable* for the SafeNoSQL prototype. *SharedTable* for d'Artagnan.
- **cryptographickey**: path to the cryptographic key to be used by the encryption schemes. Currently, SafeNoSQL only does 
not provide a cryptographic key manager.
- **schema**: path to the secure database schema file. Such file allow users to fine-tune their database schema such as 
which columns to protect, encryption schemes to use, etc. Users must be careful when defining the schema file and need 
to take into account the trade-offs between performance, security and functionalities. 

The following file exemplifies a regular configuration file to use in SafeNoSQL.

```xml
<configuration>
    <property>
        <name>hbase.zookeeper.quorum</name>
        <value>myhbasecluster.cluster.pt</value>
    </property>
    <property>
        <name>hbase.zookeeper.property.clientPort</name>
        <value>2181</value>
    </property>
    <property>
        <name>baseTable</name>
        <value>HTable</value>
    </property>
    <property>
        <name>cryptographickey</name>
        <value>/tmp/secretkeys/MySecretKey</value>
    </property>
    <property>
        <name>schema</name>
        <value>/tmp/secretschemas/MyHBaseSchema.xml</value>
    </property>
</configuration>
```

# Building a Schema file

In order to SafeNoSQL correctly protects sensitive data, users must specify a secure schema file. Such file contains the 
essential information about the database schema, as well as the security properties that one wants to achieve. 
Some schema files can be found in *resources/schemas/* as well as a [template schema file](src/main/resources/schemas/template.xml).

### Current Properties
- **Encryption Schemes**: the current encryption schemes supported by SafeNoSQL are standard (STD), deterministic (DET), 
order-preserving (OPE) and format-preserving encryption (FPE), as well as plaintext (PLT). The encryption schemes must be 
used in the default configurations for both ```<key>``` and ```<column>``` properties, as well when you want to 
overwrite such properties for specific *families* or *qualifiers* by using ```<cryptotechnique>```.

- **Padding**: the padding property allow users to maintain a common format size for all keys and columns. Currently, 
padding can be used by ```<keypadding>``` and ```<colpadding>```, using either **true** or **false**. (*Note 1*: In order 
to use this property correctly, you must also specify the format size for each property. *Note 2*: When using OPE (Boldyreva 
scheme), all values must be equal sized).

- **Format Size**: the format size enables users to specify a common size (or what size to expect) for each type of values.
(*Note 3*: when using the YCSB benchmark, the format size is used by the *value generators*.)

- **Encryption Mode**: the encryption mode option (**enable** or **disable**) allow users to specify which table must 
follow a secure scheme. 

### Default Configurations
The default configurations are __**mandatory**__ in all schema files. These properties define the default behavior of the 
database schema. For instance, if no table property is presented, all database tables will follow these scheme. If you 
want to override and fine-tune each configuration, you must specify on the respective property (e.g., ```<table>```, 
```key```,```family```).

```xml
<schema>
    <default>
        <key>PLT</key>
        <columns>PLT</columns>
        <keypadding>false</keypadding>
        <colpadding>false</colpadding>
        <keyformatsize>10</keyformatsize>
        <colformatsize>10</colformatsize>
        <encryptionmode>disable</encryptionmode>
    </default>
    ...
</schema>
```
 
### Table Configurations
Each table has its own configuration properties. Such configurations are not mandatory, but they must be used in order to 
provide a finer grain of secure configurations.

- **name**: specify the table name.
- **default**: similar to the default configurations presented above, however, they are not mandatory.
- **key**: Properties of the *Keys* (Rows) to be stored in the database. 
- **columns**: Properties of the *Columns* (Values) to be stored in the database.

#### Example of Key Properties
```xml
<key>
    <cryptotechnique>OPE</cryptotechnique>
    <formatsize>16</formatsize>
    <keypadding>true</keypadding>
</key>
        
```

#### Example of Columns Properties
```xml
<columns>
    <family>
        <name>Identification</name>
        <cryptotechnique>DET</cryptotechnique>
        <qualifier>
            <name>Main Identification</name>
            <cryptotechnique>DET</cryptotechnique>
            <colformatsize>64</colformatsize>
        </qualifier>
    </family>
</columns>    
```

# 
