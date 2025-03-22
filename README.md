# eRPC - An efficient Relay Partition Checker

A flexible tool that aims to assess partition among relays in the Tor Network.

# Features

- ☐ - Need to work on
- ☑ - Working on/Partially Complete
- ✅- Completed

Here are some current/aspiring features of the entire project

## ☑  Primary Worker

- ✅ Build circuits and distribute work to secondary worker through the gRPC server
- ✅ Uniquely identify a secondry worker by it's IP Addr and store state individually
- ✅ Handle work loss recovery after a secondary worker is assigned and secondary worker goes out of contact
- ✅ Expliclty turn off gRPC server and only use internal scanner ,vice versa and turning on both(ability of being/not being distributed)
- ✅ Support for saving output data to Neo4j Graph Database
- ✅ Handle influx of new Relays
- ✅ Support for saving output data to file based database such as Sqlite
- ✅ Support for choosing Neo4j and Sqlite as option
- ✅ Control number of parallel circuit creations at a time
- ✅ Logging
- ☑ Pause and Resume of the primary worker
- ☐ Load already performed scans from the database and start working from where things were after application session was turned off and on again
- ☐ Control the partition checking by including only specific relay or excluding specific relays based on "filter"
- ☐ Using data from OnionPerf metrics

## ✅ Secondary Worker
   - ✅ Build circuits on primary worker provided work
   - ✅ Control configuration such as parallel circuit creation attempts  by explicit assignment through env variables or through rpc call to primary worker itself.
   - ✅ Handle network failures such as secondary work getting disconnected while working on the work assigned by the primary worker and reconnecting and retries for those failures
   - ✅ Logging

To know the application's aim, project structure, architecture and how to run this application please go through the [Technical documentation](home.md)

### Running primary worker:

#### Prerequisites:

- This application was built and tested in Rust ```v1.77.0```, so we would recommend you to get at least that version. You can install rust toolchains through [here](https://www.rust-lang.org/tools/install).
- Some dynamic libraries and packages this application demands and you would need to install are : ``` libssl-dev```, ```libsqlite3-dev```, ```liblzma-dev```, ```protobuf-compiler```, ```pkg-config```
- **IMPORTANT :** Set the max limit for open file descriptors to something big if you have high no of parallel ciruit build attempts. Run ```ulimit -n 99999``` if you have low limit of open file descriptors

#### Configuration:

For configurations please look at [configure and run](home#configure-and-run-) in the WiKi or you can directly look at primary worker [config](https://gitlab.torproject.org/tpo/network-health/erpc/-/tree/main/config/primary)

#### Running :

Follow the following steps :
- Clone the repo using ```git clone https://gitlab.torproject.org/tpo/network-health/erpc```
- Enter into the root of the project using ```cd erpc```.
- You have the option of using **cargo** directly to run the application, or produce the **primary** worker binary first and then run it. We'll use the **cargo** option for now.
- To Make sure that you have the configuration files for the **primary** worker please do look at these [configuration](https://gitlab.torproject.org/tpo/network-health/erpc/-/tree/main/config/primary) files or you can simply go to the config directory for the primary worker by using ```cd config/primary``` from the root of the repo.
- You can tweak the application's functioning configurations as you want by tweaking the values set in the **Config.toml** and **.env** file.
- You can tweak the application's logging configuration by tweaking the values set in **log_config.yml**.

If you were to add the following in the [log_config.yml](https://gitlab.torproject.org/tpo/network-health/erpc/-/blob/main/config/primary/log_config.yml), then you will be able to log to both, the log file and stdout.
  ```
  root:
  level: debug
  appenders:
    - output
    - stdout
   ```
   You can discard logging to either stdout or log file by simply removing the line
   ```- output``` or ```- stdout``` in the [log_config.yml](https://gitlab.torproject.org/tpo/network-health/erpc/-/blob/main/config/primary/log_config.yml) file.

   To understand the configurations in depth, please visit [here](https://docs.rs/log4rs/latest/log4rs/).

- After you have set the configuration, you can run the **primary** worker by either running
    - ``` cargo run --release --bin primary -- --config config/primary/Config.toml --env config/primary/.env --log-config config/primary/log_config.yml```, from the **root of the project**, which allows you to load the configuration files by specifying the path to them.
    - ```cargo run``` from the **root of the project*, which allows you to load the default configuration files.
