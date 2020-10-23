# OT Platform. SMaLL plugin core.

Extendable commands for using machine learning functions in **Dispatcher App**.

## Getting Started

### Prerequisites

This package is require the SBT, and the JDK 8 installed.
It also required that the `smallplugin-sdk` module should be published locally using the `publishLocal` SBT's command.


### Command List
_apply_, _fit_ , _score_, _getdata_, _save_.

## Running the tests

sbt test

## Configuration

Configuration section `mlmodels` defines the file system type and path where models should be stored.
```hocon
mlmodels {
  fs = "file:/"
  path = "///mnt/glfs/mlmodels/"
}
``` 

Commands _apply_, _score_, _fit_ can be individually extended in corresponded sections of plugin.conf.
Each of command section should contain key as the extension's public name that be used to refer it in the query.
Special key name of `default` is used to specify default implementation of the command.
Value of the key may be just a fully-qualified name for an implementation class.
Value of the key also can contain a path to extensions configuration file.
That path should be relative to the path specified in the `configBasePath` parameter of plugin.conf and separated by the `@` symbol.

Example:
```hocon
configBasePath = "/some/root"

apply {
  model     = "algo.conf@com.example.small.ext.MyModel"
  default   = "ot.dispatcher.plugins.small.algos.apply.SavedModel"
}

fit {
  linreg    = "prod/lin.conf@com.example.small.ext.LinearRegression"
  linreg_t  = "test/lin.conf@com.example.small.ext.LinearRegression"
}
```

In this configuration example we are defined extensions for commands _apply_ and _fit_.
The _apply_ command section contains `model` implemented via "com.example.small.ext.MyModel" class with the configuration file `algo.conf`
that should be located in the `/some/root` directory.
The _apply_ section also contains default command implementation via "ot.dispatcher.plugins.small.algos.apply.SavedModel" class without any configuration files attached.
Next section _fit_ contains two cases of Linear Regression model. 
Both cases implemented via "com.example.small.ext.LinearRegression" class but have different configuration files attached.
Both cases use the `lin.conf` configuration file attached but in case of the `linreg` the file should be located in the `/some/root/prod` directory.
In the case of `linreg_t` the configuration file should be located in the `/some/root/test` directory.

## Deployment

See _Readme.md_ in root of **Software Development Kit** project.

### Extensions deployment

All the extensions jar-files is considered as an external dependencies and should be placed in the _libs_ folder of this plugin
(as described in the "Plugin files structure" section of the **dispatcher-sdk** README).


## Built With

* [SBT](https://www.scala-sbt.org) - Build tool for Scala and Java projects


## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 


## Authors
 
Dmitriy Arkhireev (darkhireev@isgneuro.com)

Nikolay Ryabykh (nryabykh@isgneuro.com)

Dmitriy Gusarov (dgusarov@ot.ru)  


## License

[OT.PLATFORM. License agreement.](LICENSE.md)


## Acknowledgments

Dmitriy Arkhireev (darkhireev@isgneuro.com)

Nikolay Ryabykh (nryabykh@isgneuro.com)  

Andrey Starchenkov (astarchenkov@isgneuro.com)