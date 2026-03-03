#pragma once

#include <string>

namespace autobot::executor::upbit {

std::string FormatNumberString(double value, int max_scale = 16);
std::string FormatPriceString(double price, double tick_size = 0.0, int max_scale = 16);
std::string FormatVolumeString(double volume, int max_scale = 16);

}  // namespace autobot::executor::upbit

