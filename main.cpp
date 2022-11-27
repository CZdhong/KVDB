#include"KVDB.h"
#include <iostream>
#include <unistd.h>

using namespace std;

int main() {
    KVDBHandler kv("kv.txt");
    while (1) {
        string str;
        cin >> str;
        if (str == "set") {
            string key, value;
            cin >> key >> value;
            kv.set(key, value);
        } else if (str == "del") {
            string key;
            cin >> key;
            if (kv.del(key) != KVDB_OK) {
                cout << "Invalid key." << endl;
            }
        } else if (str == "get") {
            string key, value;
            time_t time;
            cin >> key;
            if (kv.get(key, value, time) != KVDB_OK) {
                cout << "Invalid key." << endl;
            } else cout << value << endl;
        } else if (str == "purge") {
            kv.purge();
        } else if (str == "expires") {
            string key;
            int time;
            cin>>key>>time;
            if (kv.expires(key, time) != KVDB_OK)
                cout << "Invalid key" << endl;
        } else if (str == "stop") {
            kv.purge();
            break;
        }
    }
    return 0;
}
