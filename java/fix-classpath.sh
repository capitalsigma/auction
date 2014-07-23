deps=$(pwd)/dependencies
export CLASSPATH=$CLASSPATH:${deps}/junit-411.jar:${deps}/hamcrest-core-1.3.jar

# need .jars in classpath, plus .so in path. this takes care of both
hdf=$(pwd)/dependencies/HDF_Group/HDFView/2.10.1/lib/
export PATH=$PATH:${hdf}
export CLASSPATH=$CLASSPATH:${hdf}jarhdf5-2.10.1.jar
