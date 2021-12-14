# Deprecated: This project uses log4j-core prior to version 2.15 and thus is open to the Apache Log4j2 Remote Code Execution (RCE) Vulnerability - CVE-2021-44228 - Please use [search-wrapper-os-rest](https://github.com/GreenDelta/search-wrapper-os-rest) instead

# Search wrapper - Implementation for elasticsearch rest
This project provides an implementation for the search-wrapper API using elasticsearch rest as a search engine.

## Build from source

#### Dependent modules
In order to build the search-wrapper-es-rest module, you will need to install the search-wrapper API and search-wrapper-es first.
This is a plain Maven projects and can be installed via `mvn install`. See the
[search-wrapper](https://github.com/GreenDelta/search-wrapper) repository and [search-wrapper-es](https://github.com/GreenDelta/search-wrapper-es) for more
information.

#### Get the source code of the application
We recommend that to use Git to manage the source code but you can also download
the source code as a [zip file](https://github.com/GreenDelta/search-wrapper-es-rest/archive/main.zip).
Create a development directory (the path should not contain whitespaces):

```bash
mkdir dev
cd dev
```

and get the source code:

```bash
git clone https://github.com/GreenDelta/search-wrapper-es-rest.git
```

#### Build
Now you can build the module with `mvn install`, which will install the module in your local maven repository.