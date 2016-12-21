maprfs
======

maprfs is a lightweight Python wrapper for MapRFS_.

.. _MapRFS: https://www.mapr.com/products/mapr-fs

Running
-------

You must set a few environment variables prior to importing the `maprfs` package. For example:

.. code-block:: bash

   export JAVA_HOME=/usr/java/jdk1.8.0_101/
   export HADOOP_HOME=/opt/mapr/hadoop/hadoop-2.7.0/
   export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/java/jdk1.8.0_101/jre/lib/amd64/server