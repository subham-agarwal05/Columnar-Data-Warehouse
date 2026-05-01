CXX = g++
CXXFLAGS = -std=c++17 -O3
PUGI = pugixml.cpp

EXE = .exe

# Windows MySQL settings
MYSQL_INCLUDE = -I"C:\Program Files\MySQL\MySQL Server 9.7\include"
MYSQL_LIB = -L"C:\Program Files\MySQL\MySQL Server 9.7\lib" -lmysql

all: db_init$(EXE) db_load_data$(EXE) generate_cuboids$(EXE) load_to_mysql$(EXE) iceberg_cuboids$(EXE) load_raw_mysql$(EXE) sql_lattice$(EXE) compare_performance$(EXE) clean_cuboids$(EXE) query_cuboids$(EXE)

db_init$(EXE): db_init.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) db_init.cpp $(PUGI) -o db_init$(EXE)

db_load_data$(EXE): db_load_data.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) db_load_data.cpp $(PUGI) -o db_load_data$(EXE)

generate_cuboids$(EXE): generate_cuboids.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) generate_cuboids.cpp $(PUGI) -o generate_cuboids$(EXE) $(MYSQL_INCLUDE) $(MYSQL_LIB)

load_to_mysql$(EXE): load_to_mysql.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) load_to_mysql.cpp $(PUGI) -o load_to_mysql$(EXE) $(MYSQL_INCLUDE) $(MYSQL_LIB)

iceberg_cuboids$(EXE): iceberg_cuboids.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) iceberg_cuboids.cpp $(PUGI) -o iceberg_cuboids$(EXE)

load_raw_mysql$(EXE): load_raw_mysql.cpp
	$(CXX) $(CXXFLAGS) load_raw_mysql.cpp -o load_raw_mysql$(EXE) $(MYSQL_INCLUDE) $(MYSQL_LIB)

sql_lattice$(EXE): sql_lattice.cpp
	$(CXX) $(CXXFLAGS) sql_lattice.cpp -o sql_lattice$(EXE) $(MYSQL_INCLUDE) $(MYSQL_LIB)

compare_performance$(EXE): compare_performance.cpp
	$(CXX) $(CXXFLAGS) compare_performance.cpp -o compare_performance$(EXE)

clean_cuboids$(EXE): clean_cuboids.cpp
	$(CXX) $(CXXFLAGS) clean_cuboids.cpp -o clean_cuboids$(EXE) $(MYSQL_INCLUDE) $(MYSQL_LIB)

query_cuboids$(EXE): query_cuboids.cpp
	$(CXX) $(CXXFLAGS) query_cuboids.cpp -o query_cuboids$(EXE) $(MYSQL_INCLUDE) $(MYSQL_LIB)

clean:
	cmd /c del /Q /F db_init.exe db_load_data.exe generate_cuboids.exe load_to_mysql.exe iceberg_cuboids.exe load_raw_mysql.exe sql_lattice.exe compare_performance.exe clean_cuboids.exe query_cuboids.exe

.PHONY: all clean db_init db_load_data generate_cuboids load_to_mysql iceberg_cuboids
