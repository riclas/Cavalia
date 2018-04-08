PROJECT (Cavalia)
file (GLOB SRC_LIST "*.cpp")
ADD_EXECUTABLE (tpcc_benchmark ${SRC_LIST})

TARGET_LINK_LIBRARIES (tpcc_benchmark libdatabase.so
	#libjemalloc.so liblz4.so libcityhash.so libnuma.so
	libboost_filesystem.so libboost_system.so libboost_date_time.so libboost_thread.so)
TARGET_LINK_LIBRARIES (tpcc_benchmark pthread rt)
INSTALL (TARGETS tpcc_benchmark
	RUNTIME DESTINATION bin
	)
