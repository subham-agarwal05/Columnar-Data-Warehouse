CXX = g++
CXXFLAGS = -std=c++17 -O3
PUGI = pugixml.cpp

MYSQL_DIR = C:\Program Files\MySQL\MySQL Server 9.0
MYSQL_INCLUDE = -I"$(MYSQL_DIR)\include"
MYSQL_LIB = -L"$(MYSQL_DIR)\lib" -lmysql
MYSQL_DLL = libmysql.dll

all: db_init db_load_data generate_cuboids load_to_mysql

setup:
	@if not exist $(MYSQL_DLL) copy "$(MYSQL_DIR)\lib\$(MYSQL_DLL)" .

db_init: db_init.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) db_init.cpp $(PUGI) -o db_init.exe

db_load_data: db_load_data.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) db_load_data.cpp $(PUGI) -o db_load_data.exe

generate_cuboids: setup generate_cuboids.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) generate_cuboids.cpp $(PUGI) -o generate_cuboids.exe $(MYSQL_INCLUDE) $(MYSQL_LIB)

load_to_mysql: setup load_to_mysql.cpp $(PUGI)
	$(CXX) $(CXXFLAGS) load_to_mysql.cpp $(PUGI) -o load_to_mysql.exe $(MYSQL_INCLUDE) $(MYSQL_LIB)

clean:
	del /Q *.exe $(MYSQL_DLL) 2>nul

.PHONY: all clean setup db_init db_load_data generate_cuboids load_to_mysql
