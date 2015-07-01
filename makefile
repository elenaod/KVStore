test:
	g++ -I /home/saffi/dev/lib/boost_1_57_0/ -L/home/saffi/dev/lib/boost_1_57_0/stage/lib -std=c++0x data.cpp KVStore.cpp scratch.cpp -o kvstore -lboost_system -lboost_filesystem
