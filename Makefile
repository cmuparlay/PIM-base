CC := g++

data_generator: data_generator.cpp ./include
	$(CC) -o data_generator data_generator.cpp -Iinclude -isystem parlaylib/include -isystem argparse/include -std=c++17 -lpthread -O3

.PHONY: clean

clean:
	rm -f data_generator
	