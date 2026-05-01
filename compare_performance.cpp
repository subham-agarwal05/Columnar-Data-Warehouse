#include<bits/stdc++.h>
#include <chrono>
#include <filesystem>

using namespace std;
namespace fs = std::filesystem;

struct ExecResult {
    double time_s;
    string output;
};

ExecResult measure_and_capture(const string& cmd) {
    auto start = chrono::high_resolution_clock::now();
    
    array<char, 128> buffer;
    string result;
    unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) throw runtime_error("popen() failed!");
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        cout << buffer.data() << flush;
        result += buffer.data();
    }
    
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> diff = end-start;
    return {diff.count(), result};
}

void read_directory_metrics(const string& dir_path, vector<pair<string, size_t>>& tables, size_t& total_rows) {
    total_rows = 0;
    if (fs::exists(dir_path)) {
        for (const auto& entry : fs::directory_iterator(dir_path)) {
            string filename = entry.path().filename().string();
            if (filename.length() > 4 && filename.substr(filename.length()-4) == ".csv") {
                ifstream in(entry.path());
                string line;
                size_t lines = 0;
                while (getline(in, line)) {
                    if (!line.empty()) lines++;
                }
                size_t rows = (lines > 0) ? (lines - 1) : 0;
                tables.push_back({filename.substr(0, filename.length()-4), rows});
                total_rows += rows;
            }
        }
    }
    sort(tables.begin(), tables.end());
}

void parse_sql_metrics(const string& output, vector<pair<string, size_t>>& tables, size_t& total_rows) {
    total_rows = 0;
    stringstream ss(output);
    string line;
    while(getline(ss, line)) {
        if (!line.empty() && line.find(':') != string::npos) {
            size_t delim = line.find(':');
            string name = line.substr(0, delim);
            string val = line.substr(delim+1);
            if (name == "TOTAL") {
                try { total_rows = stoull(val); } catch(...) {}
            } else {
                try { tables.push_back({name, stoull(val)}); } catch(...) {}
            }
        }
    }
    sort(tables.begin(), tables.end());
}

void html_render_section(ofstream& out, const string& title, const ExecResult& col_res, const vector<pair<string, size_t>>& col_t, size_t col_rows, const ExecResult& sql_res, const vector<pair<string, size_t>>& sql_t, size_t sql_rows) {
    out << "<h2>" << title << " - Summary</h2>"
        << "<table><tr><th>Engine</th><th>Time Taken (Seconds)</th><th>Tables Count</th><th>Total Rows Generated</th></tr>"
        << "<tr><td>Custom Columnar DB</td><td>" << fixed << setprecision(4) << col_res.time_s << " s</td><td>" << col_t.size() << "</td><td>" << col_rows << "</td></tr>"
        << "<tr><td>MySQL Native Execution</td><td>" << fixed << setprecision(4) << sql_res.time_s << " s</td><td>" << sql_t.size() << "</td><td>" << sql_rows << "</td></tr>"
        << "</table>"
        
        << "<div class='flex-container'>"
        << "<div class='flex-child'>"
        << "<h3>Columnar DB Tables Breakdown (" << title << ")</h3>"
        << "<table><tr><th>Table Name</th><th>Row Count</th></tr>";
    for (auto& p : col_t) out << "<tr><td>" << p.first << "</td><td>" << p.second << "</td></tr>";
    out << "</table></div>"
        
        << "<div class='flex-child'>"
        << "<h3>MySQL Tables Breakdown (" << title << ")</h3>"
        << "<table><tr><th>Table Name</th><th>Row Count</th></tr>";
    for (auto& p : sql_t) out << "<tr><td>" << p.first << "</td><td>" << p.second << "</td></tr>";
    out << "</table></div></div><hr style='margin:40px 0;'>";
}

int main() {
    cout << "\n[1/4] Running Full Lattice on Columnar DB...\n";
    auto ft_col = measure_and_capture("generate_cuboids");
    vector<pair<string, size_t>> f_col_tables; size_t f_col_total;
    read_directory_metrics("Cuboids", f_col_tables, f_col_total);

    cout << "\n[2/4] Running Full Lattice on MySQL DB...\n";
    auto ft_sql = measure_and_capture("sql_lattice");
    vector<pair<string, size_t>> f_sql_tables; size_t f_sql_total;
    parse_sql_metrics(ft_sql.output, f_sql_tables, f_sql_total);

    cout << "\n[3/4] Running Iceberg Lattice (300000 SUM) on Columnar DB...\n";
    auto it_col = measure_and_capture("iceberg_cuboids 300000 SUM");
    vector<pair<string, size_t>> i_col_tables; size_t i_col_total;
    read_directory_metrics("IcebergCuboids_SUM_300000.00", i_col_tables, i_col_total);

    cout << "\n[4/4] Running Iceberg Lattice (300000 SUM) on MySQL DB...\n";
    auto it_sql = measure_and_capture("sql_lattice 300000 SUM");
    vector<pair<string, size_t>> i_sql_tables; size_t i_sql_total;
    parse_sql_metrics(it_sql.output, i_sql_tables, i_sql_total);
    
    // Output
    ofstream out("report.html");
    out << "<!DOCTYPE html><html><head><title>Performance Comparison</title>"
        << "<style>"
        << "body{font-family: Arial, sans-serif; margin: 40px; background-color:#ffffff; color:#1a1a2e;} "
        << "table{border-collapse: collapse; width: 100%; margin-bottom: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.15); background-color:#fff;} "
        << "th, td{border: 1px solid #ccc; padding: 12px; text-align: left; color:#222;} "
        << "th{background-color: #2c3e50; color: white;} "
        << "h1, h2, h3{color: #1a1a2e;}"
        << ".flex-container { display: flex; justify-content: space-between; gap: 20px; }"
        << ".flex-child { flex: 1; }"
        << "</style></head><body>"
        << "<h1>Cuboid Generation Performance Comparison</h1>";
        
    html_render_section(out, "Full Lattice Generation", ft_col, f_col_tables, f_col_total, ft_sql, f_sql_tables, f_sql_total);
    html_render_section(out, "Iceberg Generation (>= 300000 SUM)", it_col, i_col_tables, i_col_total, it_sql, i_sql_tables, i_sql_total);

    // Embed the detailed iceberg cuboid report if available
    {
        string iceberg_html_path = "IcebergCuboids_SUM_300000.00/iceberg_report.html";
        ifstream iceberg_html(iceberg_html_path);
        if (iceberg_html.is_open()) {
            out << "<hr style='margin:40px 0;'>";
            out << string((istreambuf_iterator<char>(iceberg_html)),
                           istreambuf_iterator<char>());
            iceberg_html.close();
        }
    }

    out << "</body></html>";
    out.close();
    cout << "\nReport successfully generated at report.html\n";
    return 0;
}
