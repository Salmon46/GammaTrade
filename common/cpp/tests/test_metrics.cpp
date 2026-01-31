#include "../Metrics.hpp"
#include <cassert>
#include <iostream>

void test_singleton() {
    Metrics& m1 = Metrics::instance();
    Metrics& m2 = Metrics::instance();
    assert(&m1 == &m2);
    std::cout << "Singleton test passed." << std::endl;
}

void test_metrics_registration() {
    Metrics& m = Metrics::instance();
    m.incrementTicksReceived("BTC-USD", "coinbase", "trade");
    m.setServiceStatus("test_service", 1.0);
    std::cout << "Metrics registration test passed (no crash)." << std::endl;
}

int main() {
    test_singleton();
    test_metrics_registration();
    return 0;
}
