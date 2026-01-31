#pragma once

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>

#include <memory>
#include <string>
#include <map>

class Metrics {
public:
    static Metrics& instance() {
        static Metrics instance;
        return instance;
    }

    void startServer(int port = 8000) {
        if (exposer) return; // Already started
        exposer = std::make_shared<prometheus::Exposer>("0.0.0.0:" + std::to_string(port));
        exposer->RegisterCollectable(registry);
    }

    // Counters
    void incrementTicksReceived(const std::string& symbol, const std::string& source, const std::string& type) {
        ticks_received_family.Add({{"symbol", symbol}, {"source", source}, {"type", type}}).Increment();
    }

    void incrementTicksPublished(const std::string& symbol, const std::string& source) {
        ticks_published_family.Add({{"symbol", symbol}, {"source", source}}).Increment();
    }

    void incrementSignalsReceived(const std::string& strategy_id, const std::string& symbol) {
        signals_received_family.Add({{"strategy", strategy_id}, {"symbol", symbol}}).Increment();
    }

    void incrementExecutionsProcessed(const std::string& adapter, const std::string& status) {
        executions_processed_family.Add({{"adapter", adapter}, {"status", status}}).Increment();
    }

    void recordError(const std::string& component, const std::string& error_type) {
        errors_family.Add({{"component", component}, {"error_type", error_type}}).Increment();
    }

    // Histograms
    void recordOrderLatency(const std::string& adapter, double seconds) {
        order_latency_family.Add({{"adapter", adapter}}, 
                                  prometheus::Histogram::BucketBoundaries{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0})
                                  .Observe(seconds);
    }

    void recordSignalLatency(const std::string& strategy_id, double seconds) {
        signal_latency_family.Add({{"strategy", strategy_id}}, 
                                   prometheus::Histogram::BucketBoundaries{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1})
                                   .Observe(seconds);
    }

    // Gauges
    void setServiceStatus(const std::string& service, double status) {
        service_status_family.Add({{"service_name", service}}).Set(status);
    }

private:
    Metrics() : registry(std::make_shared<prometheus::Registry>()),
                // Define metric families
                ticks_received_family(prometheus::BuildCounter()
                                      .Name("gammatrade_ticks_received_total")
                                      .Help("Total number of market ticks received")
                                      .Register(*registry)),
                ticks_published_family(prometheus::BuildCounter()
                                       .Name("gammatrade_ticks_published_total")
                                       .Help("Total number of ticks published")
                                       .Register(*registry)),
                signals_received_family(prometheus::BuildCounter()
                                       .Name("gammatrade_signals_received_total")
                                       .Help("Total number of trade signals received by router")
                                       .Register(*registry)),
                executions_processed_family(prometheus::BuildCounter()
                                            .Name("gammatrade_executions_processed_total")
                                            .Help("Total number of execution reports processed")
                                            .Register(*registry)),
                errors_family(prometheus::BuildCounter()
                              .Name("gammatrade_errors_total")
                              .Help("Total number of errors encountered")
                              .Register(*registry)),
                order_latency_family(prometheus::BuildHistogram()
                                     .Name("gammatrade_order_latency_seconds")
                                     .Help("Latency of order execution in seconds")
                                     .Register(*registry)),
                signal_latency_family(prometheus::BuildHistogram()
                                      .Name("gammatrade_signal_latency_seconds")
                                      .Help("Latency of signal generation in seconds")
                                      .Register(*registry)),
                service_status_family(prometheus::BuildGauge()
                                      .Name("gammatrade_service_status")
                                      .Help("Service status (1=Up, 0=Down)")
                                      .Register(*registry))
    {}

    std::shared_ptr<prometheus::Exposer> exposer;
    std::shared_ptr<prometheus::Registry> registry;

    prometheus::Family<prometheus::Counter>& ticks_received_family;
    prometheus::Family<prometheus::Counter>& ticks_published_family;
    prometheus::Family<prometheus::Counter>& signals_received_family;
    prometheus::Family<prometheus::Counter>& executions_processed_family;
    prometheus::Family<prometheus::Counter>& errors_family;
    prometheus::Family<prometheus::Histogram>& order_latency_family;
    prometheus::Family<prometheus::Histogram>& signal_latency_family;
    prometheus::Family<prometheus::Gauge>& service_status_family;
};
