#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <thread>
#include "config.h"
#include "db-handler.h"

using namespace std;

// config::finnhubApiKey

// Globals
float priceChangeThreshold = 20;
float initialCapital = 500; // in USD

float getLatestPrediction() {
    return 2536.5464;
}

int main() {
    cout << "Trading Bot";
    return 0;
}
