#include "upbit/number_string.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <locale>
#include <sstream>
#include <stdexcept>

namespace autobot::executor::upbit {

namespace {

std::string TrimTrailingZeros(std::string text) {
  const std::size_t dot = text.find('.');
  if (dot == std::string::npos) {
    return text;
  }
  while (!text.empty() && text.back() == '0') {
    text.pop_back();
  }
  if (!text.empty() && text.back() == '.') {
    text.pop_back();
  }
  if (text.empty() || text == "-0") {
    return "0";
  }
  return text;
}

double RoundToTick(double value, double tick_size) {
  if (tick_size <= 0.0) {
    return value;
  }
  const double scaled = value / tick_size;
  const double rounded = std::round(scaled);
  return rounded * tick_size;
}

}  // namespace

std::string FormatNumberString(double value, int max_scale) {
  if (!std::isfinite(value)) {
    throw std::invalid_argument("number must be finite");
  }

  const int precision = std::max(0, std::min(max_scale, 16));
  std::ostringstream out;
  out.imbue(std::locale::classic());
  out << std::fixed << std::setprecision(precision) << value;
  return TrimTrailingZeros(out.str());
}

std::string FormatPriceString(double price, double tick_size, int max_scale) {
  return FormatNumberString(RoundToTick(price, tick_size), max_scale);
}

std::string FormatVolumeString(double volume, int max_scale) {
  return FormatNumberString(volume, max_scale);
}

}  // namespace autobot::executor::upbit
