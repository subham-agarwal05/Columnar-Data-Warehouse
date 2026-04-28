#include<bits/stdc++.h>
#include <mysql.h>
#include <filesystem>

using namespace std;
namespace fs = std::filesystem;

struct Config {
    string host     = "127.0.0.1";
    int port        = 3306;
    string user     = "root";
    string password = "root123";
    string database = "cuboid_warehouse";
};

class MySQLConnection {
private:
    MYSQL* conn;
public:
    MySQLConnection() : conn(nullptr) {}
    bool connect(const Config& cfg) {
        conn = mysql_init(nullptr);
        if (!mysql_real_connect(conn, cfg.host.c_str(), cfg.user.c_str(),
                                cfg.password.c_str(), nullptr, cfg.port,
                                nullptr, 0)) {
            cerr << "MySQL connection failed: " << mysql_error(conn) << endl;
            return false;
        }
        return true;
    }
    vector<string> query(const string& q) {
        vector<string> rows;
        if (mysql_query(conn, q.c_str())) return rows;
        MYSQL_RES* result = mysql_store_result(conn);
        if (!result) return rows;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            if (row[0]) rows.push_back(row[0]);
        }
        mysql_free_result(result);
        return rows;
    }
    bool execute(const string& query) {
        if (mysql_query(conn, query.c_str())) return false;
        return true;
    }
    void close() {
        if (conn) {
            mysql_close(conn);
            conn = nullptr;
        }
    }
    ~MySQLConnection() { close(); }
};

int main() {
    cout << "Cleaning Columnar Cuboids from 'Cuboids/'..." << endl;
    if (fs::exists("Cuboids")) {
        int files_deleted = 0;
        for (const auto& entry : fs::directory_iterator("Cuboids")) {
            if (entry.path().extension() == ".csv") {
                fs::remove(entry.path());
                files_deleted++;
            }
        }
        cout << "Deleted " << files_deleted << " local CSV cuboid files." << endl;
    } else {
        cout << "No 'Cuboids' directory found locally." << endl;
    }

    cout << "Cleaning SQL Cuboids from database 'cuboid_warehouse'..." << endl;
    Config cfg;
    MySQLConnection db;
    if (!db.connect(cfg)) return 1;
    db.execute("USE " + cfg.database + ";");

    string get_tables_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'cuboid_warehouse' AND table_name LIKE 'cuboid_%';";
    vector<string> tables = db.query(get_tables_sql);
    
    if (tables.empty()) {
        cout << "No matching tables found in MySQL database." << endl;
    } else {
        int tables_dropped = 0;
        for (const string& t : tables) {
            if (db.execute("DROP TABLE `" + t + "`;")) {
                tables_dropped++;
            } else {
                cerr << "Failed to drop table: " << t << endl;
            }
        }
        cout << "Dropped " << tables_dropped << " matching MySQL tables." << endl;
    }
    
    if (fs::exists("DB/.cuboid_offset")) {
        fs::remove("DB/.cuboid_offset");
        cout << "Reset DB/.cuboid_offset tracking file." << endl;
    }

    cout << "Cleanup completed successfully!" << endl;
    return 0;
}
