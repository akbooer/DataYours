# DataYours - a pure Lua implementation of the Graphite / Carbon Whisper database system

DataYours is a plugin to acquire, store, and display data about Vera devices and measurements.  The plugin itself is really just a framework, library, and launcher for other modules (‘daemons’) which implement the real functionality. 

Four modules (DataWatcher, DataCache, DataGraph, DataDash) provide an implementation of the open-source Graphite system for storing and plotting time-based data.  According to the official documentation: _“Graphite is an enterprise-scale monitoring tool that runs well on cheap hardware.”_   This has been re-engineered in pure Lua to run on the MiOS / Vera system, and packaged as a DataYours.  Additionally, a DataMineServer module brings a graphical interface to dataMine channels and graphs within the  DataYours environment.

The real-time aspects of the system (data capture and storage) are handled by the data ‘daemons’ which take up very little space and cpu resources.  The Graphite system database is called Whisper and is a ‘round-robin’ structure: that is, it only stores data for a finite time (and on uniform increments of sampling time - the finest resolution being one second) and never grows its disk space usage (ie. all storage is pre-allocated.)  A key feature of Whisper is that it supports multiple archives with different retentions (maximum duration) and resolutions.  This effectively enables data compression of what might otherwise be very large database files.

The architecture supports data acquisition and storage over multiple Veras (which need not be ‘bridged’) and uses a very low-overhead communication protocol (UDP) which is also able to communicate with external databases (such as syslog.)  Utilising the CIFS system (separately installed) the Whisper database may be hosted on an external NAS.  A ‘dashboard’ web server provides a user interface and presentation of graphics, but this is quite separate from the underlying system and could easily be replaced by some other implementation.

The database only stores numerical data, although the front-end data capture and syslog forwarding also works for any string data type, and a conversion lookup table can be provided on a per-variable basis to convert to numeric values.  No special plotting is provided for energy usage, although a DataWatcher option is to use the built-in ‘live_energy_usage’ Luup functionality to report individual power usage on a periodic basis. 
 
For easy integration with dataMine, the dashboard provides read-only access to all the dataMine database including stored graphs, and has the capability to plot data from both the dataMine and Whisper databases.

For easy integration with AltUI, the DataWatcher daemon registers itself as a local Data Storage Provider, enabling data to be recoded and logged through the AltUI interface.
