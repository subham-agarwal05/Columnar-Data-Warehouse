#include<bits/stdc++.h>
#include <mysql.h>
using namespace std;
namespace fs = filesystem;

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
        mysql_options(conn, MYSQL_OPT_LOCAL_INFILE, nullptr);
        if (!mysql_real_connect(conn, cfg.host.c_str(), cfg.user.c_str(),
                                cfg.password.c_str(), nullptr, cfg.port,
                                nullptr, CLIENT_LOCAL_FILES)) {
            cerr << "MySQL connection failed: " << mysql_error(conn) << endl;
            return false;
        }
        return true;
    }
    bool execute(const string& query) {
        if (mysql_query(conn, query.c_str())) {
            cerr << "Query failed: " << mysql_error(conn) << "\n" << query.substr(0, 100) << endl;
            return false;
        }
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
    Config cfg;
    MySQLConnection db;
    if (!db.connect(cfg)) return 1;

    db.execute("CREATE DATABASE IF NOT EXISTS " + cfg.database + ";");
    db.execute("USE " + cfg.database + ";");

    string create_table = "CREATE TABLE IF NOT EXISTS fact_sales_raw ("
        "sales_id VARCHAR(255), sales_date VARCHAR(255), total_amount DOUBLE, "
        "customer_id VARCHAR(255), first_name VARCHAR(255), last_name VARCHAR(255), "
        "email VARCHAR(255), residential_location VARCHAR(255), customer_segment VARCHAR(255), "
        "product_id VARCHAR(255), product_name VARCHAR(255), category VARCHAR(255), "
        "brand VARCHAR(255), origin_location VARCHAR(255), store_id VARCHAR(255), "
        "store_name VARCHAR(255), store_type VARCHAR(255), store_location VARCHAR(255), "
        "salesperson_id VARCHAR(255), salesperson_name VARCHAR(255), salesperson_role VARCHAR(255), "
        "campaign_id VARCHAR(255), campaign_name VARCHAR(255), campaign_budget DOUBLE, "
        "store_manager_id VARCHAR(255), store_manager_name VARCHAR(255), store_manager_role VARCHAR(255), "
        "campaign_start_date VARCHAR(255), campaign_start_year INT, campaign_start_month INT, "
        "campaign_start_day INT, campaign_start_weekday INT, campaign_start_quarter INT, "
        "campaign_end_date VARCHAR(255), campaign_end_year INT, campaign_end_month INT, "
        "campaign_end_day INT, campaign_end_weekday INT, campaign_end_quarter INT"
    ");";

    db.execute("DROP TABLE IF EXISTS fact_sales_raw;");
    db.execute(create_table);

    string abs_path = fs::absolute("Data/fact_sales_denormalized_generated.csv").string();
    replace(abs_path.begin(), abs_path.end(), '\\', '/');

    cout << "Loading raw denormalized data into MySQL table 'fact_sales_raw'..." << endl;
    string load_query = "LOAD DATA LOCAL INFILE '" + abs_path + "' "
                         "INTO TABLE fact_sales_raw "
                         "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\\\"' "
                         "LINES TERMINATED BY '\\n' "
                         "IGNORE 1 LINES;";

    if (db.execute(load_query)) {
        cout << "Loaded successfully!" << endl;
    } else {
        cerr << "Failed to load" << endl;
        return 1;
    }
    return 0;
}
