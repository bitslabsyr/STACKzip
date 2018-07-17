# STACKzip

STACKzip is a tool that complements [STACK](https://github.com/bitslabsyr/stack), compressing data collected by STACK and moving it to an archive drive. It also allows you to compress and move data collected by [twitter-scraper-mongo](https://github.com/bitslabsyr/twitter-scraper-mongo).   

Compressing data often will use a lot of CPU. You may want to schedule STACKzip to run at a time when you expect your machine to have free CPU resources (using the time argument). STACKzip also uses [nice](https://en.wikipedia.org/wiki/Nice_(Unix)) to reduce its priority, so other processes will have first dibs on CPU time.

STACKzip takes a number of CLI arguments. Those are described below, and you can get help with them by running `python zipper.py --help`.    

### Installation and setup

To run STACKzip:  
1) Clone the code to your server using `git clone https://github.com/bitslabsyr/STACKzip.git`.    
2) Modify several parameters in [zipper.py](https://github.com/bitslabsyr/STACKzip/blob/master/zipper.py): 
   * Modify the values in [mongo_auth](https://github.com/bitslabsyr/STACKzip/blob/master/zipper.py#L21) as appropriate.  
     * If your instance of Mongo is password-protected, make sure "AUTH" is true and specify the Mongo username and password. If your instance of Mongo is not password-protected, make sure "AUTH" is false.
3) Run STACKzip with `sudo python3 zipper.py` along with the arguments you want to use.
    * Two arguments are required:  
      1) Server name: "-n MyServerName"
      2) Hour (out of 24) when the archiving should happen: "-t 23"
    * Four arguments are optional:
      1) Delete raw data files after they are compressed: "-d"
      2) Move the tar.gz file to the archive drive: "-a"
         * NOTE: If you use this parameter, confirm that the [archive drive path](https://github.com/bitslabsyr/STACKzip/blob/master/zipper.py#L28) is correct.
      3) Use Mongo to identify active STACK projects and automatically find data for those projects: "-m"
         * NOTE: If you use this parameter, STACKzip will assume that all of the data in active projects was collected with a single install of STACK. If you have data in different STACK installs, this option will not work correctly.
         * NOTE: If you do not use this argument, you *must* specify the [paths to the directories](https://github.com/bitslabsyr/STACKzip/blob/master/zipper.py#L29) that contain data you want to compress.
      4) Specify directory where STACK is installed (if not /home/bits/stack): "-s /path/to/stack"  
         * NOTE: Providing this argument will overwrite the [directory where STACKzip expects to find STACK](https://github.com/bitslabsyr/STACKzip/blob/master/zipper.py#L29). You can also change that default path in the code rather than providing it as a CLI argument.
    * If you want to handle timeline data rather than data collected by STACK:
      1) Manual: "-M /path/to/timeline/data"
      2) Manual name: "-N timeline-data"
         * NOTE: If you provide -M but not -N, STACKzip will throw an error and quit.    
    * STACKzip will continue to run with the settings you give it indefinitely, until you kill the process.
    
### Requirements

STACKzip was developed and tested with Python3.
