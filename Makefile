CXX = g++
CXXFLAGS = -std=c++17 -O3
PUGI = pugixml.cpp

# Linux MySQL settings
MYSQL_INCLUDE = -I/usr/include/mysql
MYSQL_LIB = -L/usr/lib/x86_64-linux-gnu -lmysqlclient

all: db_init db_load_data generate_cuboids load_to_mysql iceberg_cuboids load_raw_mysql sql_lattice compare_performance clean_cuboids query_cuboids

db_init: db_init.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) db_init.cpp $(PUGI) -o db_init

db_load_data: db_load_data.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) db_load_data.cpp $(PUGI) -o db_load_data

generate_cuboids: generate_cuboids.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) generate_cuboids.cpp $(PUGI) -o generate_cuboids $(MYSQL_INCLUDE) $(MYSQL_LIB)

load_to_mysql: load_to_mysql.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) load_to_mysql.cpp $(PUGI) -o load_to_mysql $(MYSQL_INCLUDE) $(MYSQL_LIB)

iceberg_cuboids: iceberg_cuboids.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) iceberg_cuboids.cpp $(PUGI) -o iceberg_cuboids

load_raw_mysql: load_raw_mysql.cpp
	$(CXX) $(CXXFLAGS) load_raw_mysql.cpp -o load_raw_mysql $(MYSQL_INCLUDE) $(MYSQL_LIB)

sql_lattice: sql_lattice.cpp
	$(CXX) $(CXXFLAGS) sql_lattice.cpp -o sql_lattice $(MYSQL_INCLUDE) $(MYSQL_LIB)

compare_performance: compare_performance.cpp
	$(CXX) $(CXXFLAGS) compare_performance.cpp -o compare_performance

clean_cuboids: clean_cuboids.cpp
	$(CXX) $(CXXFLAGS) clean_cuboids.cpp -o clean_cuboids $(MYSQL_INCLUDE) $(MYSQL_LIB)

query_cuboids: query_cuboids.cpp
	$(CXX) $(CXXFLAGS) query_cuboids.cpp -o query_cuboids $(MYSQL_INCLUDE) $(MYSQL_LIB)

clean:
	rm -f db_init db_load_data generate_cuboids load_to_mysql iceberg_cuboids load_raw_mysql sql_lattice compare_performance clean_cuboids query_cuboids

.PHONY: all clean db_init db_load_data generate_cuboids load_to_mysql iceberg_cuboids
