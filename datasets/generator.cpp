// This file is adapted from PGM-index <https://github.com/gvinciguerra/PGM-index>.
// Original work Copyright (c) 2018 Giorgio Vinciguerra.
// Modified work Copyright 2022 Chaichon Wongkham
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <random>
#include <stdexcept>
#include <stdlib.h>
#include <type_traits>
#include <utility>
#include <vector>

template <typename T>
using LargeSigned = typename std::conditional_t<
    std::is_floating_point_v<T>, long double,
    std::conditional_t<(sizeof(T) < 8), int64_t, __int128>>;

template <typename X, typename Y> class OptimalPiecewiseLinearModel {
private:
  using SX = LargeSigned<X>;
  using SY = LargeSigned<Y>;

  struct Slope {
    SX dx{};
    SY dy{};

    bool operator<(const Slope &p) const { return dy * p.dx < dx * p.dy; }
    bool operator>(const Slope &p) const { return dy * p.dx > dx * p.dy; }
    bool operator==(const Slope &p) const { return dy * p.dx == dx * p.dy; }
    bool operator!=(const Slope &p) const { return dy * p.dx != dx * p.dy; }
    explicit operator long double() const { return dy / (long double)dx; }
  };

  struct Point {
    X x{};
    Y y{};

    Slope operator-(const Point &p) const { return {SX(x) - p.x, SY(y) - p.y}; }
  };

  const Y epsilon;
  std::vector<Point> lower;
  std::vector<Point> upper;
  X first_x = 0;
  X last_x = 0;
  size_t lower_start = 0;
  size_t upper_start = 0;
  size_t points_in_hull = 0;
  Point rectangle[4];

  auto cross(const Point &O, const Point &A, const Point &B) const {
    auto OA = A - O;
    auto OB = B - O;
    return OA.dx * OB.dy - OA.dy * OB.dx;
  }

public:
  class CanonicalSegment;

  explicit OptimalPiecewiseLinearModel(Y epsilon)
      : epsilon(epsilon), lower(), upper() {
    if (epsilon < 0)
      throw std::invalid_argument("epsilon cannot be negative");

    upper.reserve(1u << 16);
    lower.reserve(1u << 16);
  }

  bool check_point(const X &x, const Y &y) {
    // if (points_in_hull > 0 && x <= last_x)
    //   throw std::logic_error("CHECK: Points must be increasing by x.");

    // last_x = x;
    auto max_y = std::numeric_limits<Y>::max();
    auto min_y = std::numeric_limits<Y>::lowest();
    Point p1{x, y >= max_y - epsilon ? max_y : y + epsilon};
    Point p2{x, y <= min_y + epsilon ? min_y : y - epsilon};

    if (points_in_hull == 0) {
      return true;
    }

    if (points_in_hull == 1) {
      return true;
    }

    auto slope1 = rectangle[2] - rectangle[0];
    auto slope2 = rectangle[3] - rectangle[1];
    bool outside_line1 = p1 - rectangle[2] < slope1;
    bool outside_line2 = p2 - rectangle[3] > slope2;

    if (outside_line1 || outside_line2) {
      points_in_hull = 0;
      return false;
    } else
      return true;
  }

  bool add_point(const X &x, const Y &y) {
    if (points_in_hull > 0 && x <= last_x) {
      std::cout << "points_in_hull: " << points_in_hull << std::endl;
      std::cout << "x = " << x << std::endl;
      std::cout << "last_x = " << last_x << std::endl;
      throw std::logic_error("ADD: Points must be increasing by x.");
    }

    last_x = x;
    auto max_y = std::numeric_limits<Y>::max();
    auto min_y = std::numeric_limits<Y>::lowest();
    Point p1{x, y >= max_y - epsilon ? max_y : y + epsilon};
    Point p2{x, y <= min_y + epsilon ? min_y : y - epsilon};

    if (points_in_hull == 0) {
      first_x = x;
      rectangle[0] = p1;
      rectangle[1] = p2;
      upper.clear();
      lower.clear();
      upper.push_back(p1);
      lower.push_back(p2);
      upper_start = lower_start = 0;
      ++points_in_hull;
      return true;
    }

    if (points_in_hull == 1) {
      rectangle[2] = p2;
      rectangle[3] = p1;
      upper.push_back(p1);
      lower.push_back(p2);
      ++points_in_hull;
      return true;
    }

    auto slope1 = rectangle[2] - rectangle[0];
    auto slope2 = rectangle[3] - rectangle[1];
    bool outside_line1 = p1 - rectangle[2] < slope1;
    bool outside_line2 = p2 - rectangle[3] > slope2;

    if (outside_line1 || outside_line2) {
      points_in_hull = 0;
      return false;
    }

    if (p1 - rectangle[1] < slope2) {
      // Find extreme slope
      auto min = lower[lower_start] - p1;
      auto min_i = lower_start;
      for (auto i = lower_start + 1; i < lower.size(); i++) {
        auto val = lower[i] - p1;
        if (val > min)
          break;
        min = val;
        min_i = i;
      }

      rectangle[1] = lower[min_i];
      rectangle[3] = p1;
      lower_start = min_i;

      // Hull update
      auto end = upper.size();
      for (; end >= upper_start + 2 &&
             cross(upper[end - 2], upper[end - 1], p1) <= 0;
           --end)
        continue;
      upper.resize(end);
      upper.push_back(p1);
    }

    if (p2 - rectangle[0] > slope1) {
      // Find extreme slope
      auto max = upper[upper_start] - p2;
      auto max_i = upper_start;
      for (auto i = upper_start + 1; i < upper.size(); i++) {
        auto val = upper[i] - p2;
        if (val < max)
          break;
        max = val;
        max_i = i;
      }

      rectangle[0] = upper[max_i];
      rectangle[2] = p2;
      upper_start = max_i;

      // Hull update
      auto end = lower.size();
      for (; end >= lower_start + 2 &&
             cross(lower[end - 2], lower[end - 1], p2) >= 0;
           --end)
        continue;
      lower.resize(end);
      lower.push_back(p2);
    }

    ++points_in_hull;
    return true;
  }

  CanonicalSegment get_segment() {
    if (points_in_hull == 1)
      return CanonicalSegment(rectangle[0], rectangle[1], first_x);
    return CanonicalSegment(rectangle, first_x);
  }

  void reset() {
    points_in_hull = 0;
    lower.clear();
    upper.clear();
  }
};

template <typename X, typename Y>
class OptimalPiecewiseLinearModel<X, Y>::CanonicalSegment {
  friend class OptimalPiecewiseLinearModel;

  Point rectangle[4];
  X first;

  CanonicalSegment(const Point &p0, const Point &p1, X first)
      : rectangle{p0, p1, p0, p1}, first(first){};

  CanonicalSegment(const Point (&rectangle)[4], X first)
      : rectangle{rectangle[0], rectangle[1], rectangle[2], rectangle[3]},
        first(first){};

  bool one_point() const {
    return rectangle[0].x == rectangle[2].x &&
           rectangle[0].y == rectangle[2].y &&
           rectangle[1].x == rectangle[3].x && rectangle[1].y == rectangle[3].y;
  }

public:
  CanonicalSegment() = default;

  X get_first_x() const { return first; }

  std::pair<long double, long double> get_intersection() const {
    auto &p0 = rectangle[0];
    auto &p1 = rectangle[1];
    auto &p2 = rectangle[2];
    auto &p3 = rectangle[3];
    auto slope1 = p2 - p0;
    auto slope2 = p3 - p1;

    if (one_point() || slope1 == slope2)
      return {p0.x, p0.y};

    auto p0p1 = p1 - p0;
    auto a = slope1.dx * slope2.dy - slope1.dy * slope2.dx;
    auto b = (p0p1.dx * slope2.dy - p0p1.dy * slope2.dx) /
             static_cast<long double>(a);
    auto i_x = p0.x + b * slope1.dx;
    auto i_y = p0.y + b * slope1.dy;
    return {i_x, i_y};
  }

  std::pair<long double, SY> get_floating_point_segment(const X &origin) const {
    if (one_point())
      return {0, (rectangle[0].y + rectangle[1].y) / 2};

    if constexpr (std::is_integral_v<X> && std::is_integral_v<Y>) {
      auto slope = rectangle[3] - rectangle[1];
      auto intercept_n = slope.dy * (SX(origin) - rectangle[1].x);
      auto intercept_d = slope.dx;
      auto rounding_term =
          ((intercept_n < 0) ^ (intercept_d < 0) ? -1 : +1) * intercept_d / 2;
      auto intercept =
          (intercept_n + rounding_term) / intercept_d + rectangle[1].y;
      return {static_cast<long double>(slope), intercept};
    }

    auto [i_x, i_y] = get_intersection();
    auto [min_slope, max_slope] = get_slope_range();
    auto slope = (min_slope + max_slope) / 2.;
    auto intercept = i_y - (i_x - origin) * slope;
    return {slope, intercept};
  }

  std::pair<long double, long double> get_slope_range() const {
    if (one_point())
      return {0, 1};

    auto min_slope = static_cast<long double>(rectangle[2] - rectangle[0]);
    auto max_slope = static_cast<long double>(rectangle[3] - rectangle[1]);
    return {min_slope, max_slope};
  }
};

template <typename KEY_TYPE>
size_t PGM_metric(KEY_TYPE *keys, int key_num, int error_bound) {
  OptimalPiecewiseLinearModel<KEY_TYPE, uint64_t> segment(error_bound);
  std::sort(keys, keys + key_num);
  size_t metric = 1;
  double max = 0;
  for (auto i = 0; i < key_num; i++) {
    if (!segment.add_point(keys[i], i)) {
      metric++;
      segment.reset();
      segment.add_point(keys[i], i);
    }
  }
  return metric;
}

// Generate the keys based on one global slope and one local slope // Based on
// two constraint
template <typename X, typename Y> class ConstraintGenerator {
public:
  using SX = LargeSigned<X>;
  using SY = LargeSigned<Y>;

  struct Slope {
    SX dx{};
    SY dy{};

    bool operator<(const Slope &p) const { return dy * p.dx < dx * p.dy; }
    bool operator>(const Slope &p) const { return dy * p.dx > dx * p.dy; }
    bool operator==(const Slope &p) const { return dy * p.dx == dx * p.dy; }
    bool operator!=(const Slope &p) const { return dy * p.dx != dx * p.dy; }
    explicit operator long double() const { return dy / (long double)dx; }
  };

  struct Point {
    X x{};
    Y y{};

    Slope operator-(const Point &p) const { return {SX(x) - p.x, SY(y) - p.y}; }
  };
  X last_x_ = 0;
  Point global_origin_{0,
                       0};   // it must be the origin point of global slope line
  Point local_origin_{0, 0}; // it must be one point in the global slope line
  Y local_epsilon_;
  Y global_epsilon_;
  Slope local_slope_;
  Slope global_slope_;
  long double global_intercept_;
  long double local_intercept_;
  int64_t global_num_keys_;
  int64_t local_num_keys_;
  std::random_device rd;
  std::mt19937 gen; // Standard mersenne_twister_engine seeded with rd()
  std::uniform_int_distribution<> distriby;
  std::uniform_int_distribution<> distribx;
  std::uniform_int_distribution<> random_gen;
  // global slope is from 0.1 to 0.002

  ConstraintGenerator(Y local_epsilon, Y global_epsilon, Y local_num_keys,
                      Y global_num_keys)
      : local_epsilon_(local_epsilon), global_epsilon_(global_epsilon),
        local_num_keys_(local_num_keys), global_num_keys_(global_num_keys),
        distriby(1, 10), distribx(101, 102), random_gen(0, 1000) {
    gen.seed(rd());
    last_x_ = 0;
  }

  void set_local_epsilon(Y local_epsilon) { local_epsilon_ = local_epsilon; }
  void set_global_epsilon(Y global_epsilon) {
    global_epsilon_ = global_epsilon;
  }
  void set_local_num_keys(Y local_num_keys) {
    local_num_keys_ = local_num_keys;
  }
  void set_global_num_keys(Y global_num_keys) {
    global_num_keys_ = global_num_keys;
  }
  void set_last_x(X last_x) { last_x_ = last_x; }
  void set_global_origin(X x_, Y y_) {
    global_origin_.x = x_;
    global_origin_.y = y_;
  }
  void set_local_origin(X x_, Y y_) {
    local_origin_.x = x_;
    local_origin_.y = y_;
  }

  void generate_global_slope() {
    // Uniformly to generate the keys
    global_slope_.dx = distribx(gen);
    global_slope_.dy = distriby(gen);
    // if (static_cast<long double>(global_slope_) > 0.1 ||
    //     static_cast<long double>(global_slope_) < 0.01) {
    //   std::cout << "generated error for global slope" << std::endl;
    //   std::cout << "dx = " << static_cast<int>(global_slope_.dx) <<
    //   std::endl; std::cout << "dy = " << static_cast<int>(global_slope_.dy)
    //   << std::endl; std::cout << "global slope = " << static_cast<long
    //   double>(global_slope_)
    //             << std::endl;
    //   exit(0);
    // }
    global_intercept_ =
        global_origin_.y -
        global_origin_.x * static_cast<long double>(global_slope_);
  }

  // [local_origin.y, target_rank]
  // the slope should be more & more careful
  void generate_local_slope(Y target_rank) {
    // When generating local slope, we must make sure enough local keys could be
    // generated
    // std::cout << "Start generating local slope *******************"
    //           << std::endl;
    X min_x = ceill((target_rank - global_epsilon_ - global_intercept_) /
                    static_cast<long double>(global_slope_));
    // std::cout << "global min_x = " << min_x << std::endl;
    min_x = std::max(min_x, last_x_ + 1);
    // std::cout << "restricted min x = " << min_x << std::endl;
    X max_x = floorl((target_rank + global_epsilon_ - global_intercept_) /
                     static_cast<long double>(global_slope_));
    X mid_x = floorl((target_rank - global_intercept_) /
                     static_cast<long double>(global_slope_));
    // std::cout << "global mid_x = " << mid_x << std::endl;
    // std::cout << "global max_x = " << max_x << std::endl;

    if (mid_x <= min_x) {
      mid_x = max_x;
    }
    X select_x = min_x + (mid_x - min_x - 1) * (random_gen(gen) / 1000.0);
    // std::cout << "select_x = " << select_x << std::endl;
    Point new_xy{select_x, target_rank};
    local_slope_ = new_xy - local_origin_;
    // std::cout << "first local slope = "
    //           << static_cast<long double>(local_slope_) << std::endl;
    // To make sure local scope could generate enough keys
    while (static_cast<long double>(local_slope_) > 0.2 &&
           select_x < mid_x - 1) {
      select_x++;
      new_xy.x = select_x;
      local_slope_ = new_xy - local_origin_;
    }
    local_intercept_ = local_origin_.y -
                       local_origin_.x * static_cast<long double>(local_slope_);
    // std::cout << "After 1st tuning: local slope = "
    //           << static_cast<long double>(local_slope_) << std::endl;
    X next_max_global =
        (target_rank + 1 + global_epsilon_ - global_intercept_) /
        static_cast<long double>(global_slope_);
    X next_max_local = (target_rank + 1 + local_epsilon_ - local_intercept_) /
                       static_cast<long double>(local_slope_);
    while (next_max_local >= next_max_global ||
           abs(next_max_global - next_max_local) <= 1) {
      // Adjust local slope with aim to differentiate
      if (select_x <= min_x)
        break;
      select_x--;
      new_xy.x = select_x;
      local_slope_ = new_xy - local_origin_;
      local_intercept_ =
          local_origin_.y -
          local_origin_.x * static_cast<long double>(local_slope_);
      next_max_local = (target_rank + 1 + local_epsilon_ - local_intercept_) /
                       static_cast<long double>(local_slope_);
    }
    // std::cout << "After 2nd tuning: local slope = "
    //           << static_cast<long double>(local_slope_) << std::endl;

    if (static_cast<long double>(local_slope_) >= 1 ||
        static_cast<long double>(local_slope_) <= 0) {
      std::cout << "cannot select an approapriate local slope = "
                << static_cast<long double>(local_slope_) << std::endl;
      exit(0);
    }

    if (next_max_local >= next_max_global ||
        abs(next_max_global - next_max_local) <= 1) {
      std::cout << "cannot select an uncovered local slope = "
                << static_cast<long double>(local_slope_) << std::endl;
      exit(0);
    }

    // std::cout << "determine local scope = "
    //           << static_cast<long double>(local_slope_) << std::endl;
    // std::cout << "next_max_local = " << next_max_local << std::endl;
    // std::cout << "next max global = " << next_max_global << std::endl;
    // std::cout << "Finish generating local slope *******************"
    //           << std::endl;
  }

  // to select the key for next local segment
  X generate_outside_local(Y rank, Y restrict_rank) {
    X next_max_global = (rank + global_epsilon_ - global_intercept_) /
                        static_cast<long double>(global_slope_);
    X restrict_global = (restrict_rank - global_intercept_) /
                        static_cast<long double>(global_slope_);
    next_max_global =
        std::min(next_max_global, restrict_global - (restrict_rank - rank) * 2);
    X next_max_local = (rank + local_epsilon_ - local_intercept_) /
                       static_cast<long double>(local_slope_);
    X select_x;
    if (next_max_global < next_max_local) {
      //   std::cout << "next max global = " << next_max_global << std::endl;
      //   std::cout << "next max local = " << next_max_local << std::endl;
      //   std::cout << "cannot find a key that is outside previous local"
      //             << std::endl;
      // exit(0);
      select_x = last_x_ + 1;
    } else {
      select_x =
          next_max_local + 1 +
          (next_max_global - next_max_local) * (random_gen(gen) / 1000.0);
      select_x = std::min(select_x, next_max_global);
    }
    // std::cout
    //     << "----------------------------------------------------------------"
    //     << std::endl;
    // std::cout << "last x = " << last_x_ << std::endl;
    // std::cout << "next max local = " << next_max_local << std::endl;
    // std::cout << "next max global = " << next_max_global << std::endl;

    // std::cout << "next x = " << select_x << std::endl;
    // std::cout << "the rank = " << rank << std::endl;
    // std::cout
    //     << "----------------------------------------------------------------"
    //     << std::endl;
    last_x_ = select_x;
    return select_x;
  }

  X generate_outside_global(Y rank) {
    X next_max_global = (rank + global_epsilon_ - global_intercept_) /
                        static_cast<long double>(global_slope_);
    last_x_ = next_max_global + 1;
    return last_x_;
  }

  X generate_key(Y rank) {
    X global_min_x = ceill((rank - global_epsilon_ - global_intercept_) /
                           static_cast<long double>(global_slope_));
    X global_max_x = floorl((rank + global_epsilon_ - global_intercept_) /
                            static_cast<long double>(global_slope_));
    X local_min_x = ceill((rank - local_epsilon_ - local_intercept_) /
                          static_cast<long double>(local_slope_));
    X local_max_x = floorl((rank + local_epsilon_ - local_intercept_) /
                           static_cast<long double>(local_slope_));
    X min_x = std::max(local_min_x, global_min_x);
    min_x = std::max(min_x, last_x_ + 1); // to guarantee increasing key
    X max_x = std::min(local_max_x, global_max_x);
    X ret = min_x + (max_x - min_x) * (random_gen(gen) / 1000.0);
    // std::cout << "min x = " << min_x << std::endl;
    // std::cout << "global_slope_ = " << static_cast<long
    // double>(global_slope_)
    //           << std::endl;
    // std::cout << "local_slope_ = " << static_cast<long double>(local_slope_)
    //           << std::endl;
    // std::cout << "min x = " << min_x << std::endl;
    // std::cout << "max x = " << max_x << std::endl;
    // std::cout << "rank " << rank << " = " << ret << std::endl;

    // std::cout << "global_epsilon_ = " << global_epsilon_ << std::endl;
    // std::cout << "local_epsilon_ = " << local_epsilon_ << std::endl;
    // std::cout << "global slope = " << static_cast<long double>(global_slope_)
    //           << std::endl;
    // std::cout << "local slope = " << static_cast<long double>(local_slope_)
    //           << std::endl;
    // std::cout << "max_x = " << max_x << std::endl;
    // std::cout << "global_min_x = " << global_min_x << std::endl;
    // std::cout << "global_max_x = " << global_max_x << std::endl;
    // std::cout << "local_min_x = " << local_min_x << std::endl;
    // std::cout << "local_max_x = " << local_max_x << std::endl;
    // std::cout << "min_x = " << min_x << std::endl;
    // std::cout << "max_x = " << max_x << std::endl;
    // std::cout << "ret_x = " << ret << std::endl;
    last_x_ = ret;
    // exit(0);
    return ret;
  }
};

void dataGenerator(int local_epsilon, int global_epsilon, int local_value,
                   int global_value, int num, std::string out_path) {
  std::cout << "local epsilon: " << local_epsilon << std::endl;
  std::cout << "global epsilon: " << global_epsilon << std::endl;
  std::cout << "local value: " << local_value << std::endl;
  std::cout << "global value: " << global_value << std::endl;

  uint64_t *data = new uint64_t[num + 1];
  data[0] = num;
  OptimalPiecewiseLinearModel<int64_t, int64_t> local_pla(local_epsilon);
  OptimalPiecewiseLinearModel<int64_t, int64_t> global_pla(global_epsilon);
  local_pla.reset();
  global_pla.reset();

  // local_value (global_value) means # model for local (global) epsilion
  if (local_epsilon > global_epsilon || local_value < global_value) {
    std::cout << "Wrong settting for the parameters!" << std::endl;
    std::cout << "Requirement: Local epsilon <= global_epilon, local_value >= "
                 "global_value"
              << std::endl;
    return;
  }

  // #keys per local model
  // #keys per gloabl model
  // #local modes per global model
  int local_models_per_global_model = local_value / global_value;
  int keys_per_global_model = num / global_value;
  int keys_per_local_model =
      keys_per_global_model / local_models_per_global_model;
  int local_models_in_last_glocal_model = local_models_per_global_model;
  int keys_in_last_global_model = keys_per_global_model;
  int keys_in_last_local_model = keys_per_local_model;

  if (local_value % global_value) {
    local_models_in_last_glocal_model += local_value % global_value;
  }

  // Handle last global segment separately
  if (num % global_value) {
    keys_in_last_global_model += num % global_value;
  }

  // Handle last local segment in one global segment separately
  if (keys_per_global_model % local_models_per_global_model) {
    keys_in_last_local_model +=
        keys_per_global_model % local_models_per_global_model;
  }

  std::cout << "local_models_per_global_model = "
            << local_models_per_global_model << std::endl;
  std::cout << "local models in last global model = "
            << local_models_in_last_glocal_model << std::endl;
  std::cout << "keys per global model = " << keys_per_global_model << std::endl;
  std::cout << "keys in last global model = " << keys_in_last_global_model
            << std::endl;
  std::cout << "keys per local model = " << keys_per_local_model << std::endl;
  std::cout << "keys_in_last_local_model = " << keys_in_last_local_model
            << std::endl;

  uint64_t cur_rank = 1;
  uint64_t cur_key = 0;
  uint64_t base_key = 0;
  // use int64_t
  ConstraintGenerator<int64_t, int64_t> my_generator(
      local_epsilon, global_epsilon, keys_per_local_model,
      keys_per_global_model);
  my_generator.generate_global_slope();
  my_generator.generate_local_slope(keys_per_local_model);
  cur_key = my_generator.generate_key(cur_rank);
  data[cur_rank] = cur_key;
  global_pla.add_point(cur_key, cur_rank);
  cur_rank++;
  // std::cout << "before the rank is " << cur_rank << std::endl;
  for (int i = 0; i < global_value - 1; ++i) {
    for (int j = 0; j < local_models_per_global_model - 1; ++j) {
      for (int k = 0; k < keys_per_local_model - 1; ++k) {
        // Need to check both the local constraint and global constraint
        cur_key = my_generator.generate_key(cur_rank);
        data[cur_rank] = cur_key;
        if (!global_pla.check_point(cur_key, cur_rank)) {
          global_pla.reset();
        }
        global_pla.add_point(cur_key, cur_rank);
        cur_rank++;
      }
      // std::cout << "pre: inside loop curr_rank = " << cur_rank << std::endl;

      if (j != local_models_per_global_model - 2) {
        cur_key = my_generator.generate_outside_local(
            cur_rank, cur_rank + keys_per_local_model - 1);
        data[cur_rank] = cur_key;
        if (!global_pla.check_point(cur_key, cur_rank)) {
          global_pla.reset();
        }
        global_pla.add_point(cur_key, cur_rank);
        my_generator.set_local_origin(cur_key, cur_rank);
        my_generator.generate_local_slope(cur_rank + keys_per_local_model - 1);
        cur_rank++;
      }
      // std::cout << "inside loop curr_rank = " << cur_rank << std::endl;
    }

    if (local_models_per_global_model != 1) {
      cur_key = my_generator.generate_outside_local(
          cur_rank, cur_rank + keys_in_last_local_model - 1);
      data[cur_rank] = cur_key;
      if (!global_pla.check_point(cur_key, cur_rank)) {
        global_pla.reset();
      }
      global_pla.add_point(cur_key, cur_rank);
      my_generator.set_local_origin(cur_key, cur_rank);
      my_generator.generate_local_slope(cur_rank + keys_in_last_local_model -
                                        1);
      cur_rank++;
    }
    // Handle last local segment
    for (int k = 0; k < keys_in_last_local_model - 1; ++k) {
      cur_key = my_generator.generate_key(cur_rank);
      data[cur_rank] = cur_key;
      if (!global_pla.check_point(cur_key, cur_rank)) {
        global_pla.reset();
      }
      global_pla.add_point(cur_key, cur_rank);
      cur_rank++;
    }

    if (i != global_value - 2) {
      cur_key = my_generator.generate_outside_global(cur_rank);
      while (global_pla.check_point(cur_key, cur_rank)) {
        ++cur_key;
      }
      global_pla.reset();
      global_pla.add_point(cur_key, cur_rank);
      data[cur_rank] = cur_key;
      my_generator.set_last_x(cur_key);
      my_generator.set_global_origin(cur_key, cur_rank);
      my_generator.set_local_origin(cur_key, cur_rank);
      my_generator.generate_global_slope();
      my_generator.generate_local_slope(cur_rank + keys_per_local_model - 1);
      cur_rank++;
    }
  }

  //   std::cout << "Except last segment: curr_rank = " << cur_rank <<
  //   std::endl;

  // Handle last global model seperately !!!!!
  keys_per_local_model =
      keys_in_last_global_model / local_models_in_last_glocal_model;
  keys_in_last_local_model = keys_per_local_model;
  if (keys_in_last_global_model % local_models_in_last_glocal_model) {
    keys_in_last_local_model +=
        keys_in_last_global_model % local_models_in_last_glocal_model;
  }

  if (global_value != 1) {
    cur_key = my_generator.generate_outside_global(cur_rank);
    while (global_pla.check_point(cur_key, cur_rank)) {
      ++cur_key;
    }
    global_pla.reset();
    global_pla.add_point(cur_key, cur_rank);
    my_generator.set_last_x(cur_key);
    data[cur_rank] = cur_key;
    my_generator.set_global_origin(cur_key, cur_rank);
    my_generator.set_local_origin(cur_key, cur_rank);
    my_generator.generate_global_slope();
    my_generator.generate_local_slope(cur_rank + keys_per_local_model - 1);
    cur_rank++;
  }

  for (int j = 0; j < local_models_in_last_glocal_model - 1; ++j) {
    for (int k = 0; k < keys_per_local_model - 1; ++k) {
      cur_key = my_generator.generate_key(cur_rank);
      data[cur_rank] = cur_key;
      cur_rank++;
    }

    //  std::cout << "pre 2: inside loop curr_rank = " << cur_rank << std::endl;

    if (j != local_models_in_last_glocal_model - 2) {
      cur_key = my_generator.generate_outside_local(
          cur_rank, cur_rank + keys_per_local_model - 1);
      data[cur_rank] = cur_key;
      my_generator.set_local_origin(cur_key, cur_rank);
      my_generator.generate_local_slope(cur_rank + keys_per_local_model - 1);
      cur_rank++;
    }
    // std::cout << "end 2: inside loop curr_rank = " << cur_rank << std::endl;
  }

  if (local_models_in_last_glocal_model != 1) {
    cur_key = my_generator.generate_outside_local(
        cur_rank, cur_rank + keys_in_last_local_model - 1);
    data[cur_rank] = cur_key;
    my_generator.set_local_origin(cur_key, cur_rank);
    my_generator.generate_local_slope(cur_rank + keys_in_last_local_model - 1);
    cur_rank++;
  }

  // Handle last local segment
  for (int k = 0; k < keys_in_last_local_model - 1; ++k) {
    cur_key = my_generator.generate_key(cur_rank);
    data[cur_rank] = cur_key;
    cur_rank++;
  }

  std::cout << "cur_rank = " << cur_rank << std::endl;
  std::cout << "num = " << num << std::endl;

  //   for (int i = 1; i < num; ++i) {
  //     std::cout << i << " : " << data[i] << std::endl;
  //   }
  std::cout << "start test increasing order" << std::endl;
  for (int i = 1; i < num; ++i) {
    if (data[i + 1] <= data[i]) {
      std::cout << "i = " << i << std::endl;
      std::cout << "high = " << data[i + 1] << std::endl;
      std::cout << "low = " << data[i] << std::endl;
      exit(0);
    }
  }
  std::cout << "Finish test increasing order" << std::endl;

  local_pla.reset();
  global_pla.reset();
  int generated_global_value = 1;
  int generated_local_value = 1;
  local_pla.add_point(data[1], 1);
  global_pla.add_point(data[1], 1);
  //   std::cout << 1 << " : " << data[1] << std::endl;

  for (int i = 2; i <= num; ++i) {
    // std::cout << i << " : " << data[i] << std::endl;
    if (!local_pla.check_point(data[i], i)) {
      ++generated_local_value;
      local_pla.reset();
    }
    local_pla.add_point(data[i], i);
    if (!global_pla.check_point(data[i], i)) {
      ++generated_global_value;
      global_pla.reset();
    }
    global_pla.add_point(data[i], i);
  }

  std::cout << "Real newly generated global segment =  "
            << generated_global_value << std::endl;
  std::cout << "Real newly generated local segment =  " << generated_local_value
            << std::endl;
  if (out_path.size() > 0) {
    std::fstream file(out_path, std::ios::out | std::ios::binary);
    if (!file.is_open())
      std::cout << "cannot write file" << std::endl;
    file.write(reinterpret_cast<char *>(data), sizeof(uint64_t) * (num + 1));
    file.close();
    std::cout << "file written to " << out_path << std::endl;
  }

  //   for (int i = 0; i < num; ++i) {
  //     std::cout << data[i] << std::endl;
  //   }

  // To check how many segments are generated

  //   // local pass
  //   std::cout << "--- local pass ---" << std::endl;
  //   int key_per_model = num / local_value;
  //   uint64_t curr_rank = 1;
  //   uint64_t curr_key = 0;
  //   for (int i = 0; i < local_value; i++) {
  //     std::cout << "\r" << i + 1 << "/" << local_value << std::flush;
  //     int j;
  //     // increasing key
  //     for (j = 1; j <= key_per_model; j++) {
  //       data[curr_rank++] = curr_key;
  //       local_pla.add_point(curr_key, j);
  //       curr_key++;
  //     }
  //     // now find the 'out of bound' key
  //     while (local_pla.check_point(curr_key++, j))
  //       ;
  //     local_pla.reset();
  //   }
  //   // naive extend left over
  //   if (num % local_value != 0) {
  //     for (; curr_rank <= num + 1; curr_rank++) {
  //       data[curr_rank] = curr_key++;
  //     }
  //   }
  //   std::cout << std::endl;

  //   // global pass
  //   std::cout << "--- global pass ---" << std::endl;
  //   key_per_model = num / global_value;
  //   curr_rank = 1;
  //   uint64_t offset = 0;
  //   for (int i = 0; i < global_value; i++) {
  //     std::cout << "\r" << i + 1 << "/" << global_value << std::flush;
  //     int j;
  //     // key from local
  //     for (j = 1; j <= key_per_model; j++) {
  //       curr_key = data[curr_rank++];
  //       if (!global_pla.add_point(curr_key, j)) {
  //         std::cout << "global failed to encapsulate local" << std::endl;
  //         return;
  //       }
  //     }
  //     // now find the 'out of bound' key
  //     while (global_pla.check_point(++curr_key, j)) {
  //       offset++;
  //     }
  //     for (int k = curr_rank; k < num + 1; k++)
  //       data[k] += offset;
  //     global_pla.reset();
  //     offset = 0;
  //   }
  //   std::cout << std::endl;
}

int main(int argc, char **argv) {
  dataGenerator(strtol(argv[1], nullptr, 0), strtol(argv[2], nullptr, 0),
                strtol(argv[3], nullptr, 0), strtol(argv[4], nullptr, 0),
                strtol(argv[5], nullptr, 0), argv[6]);
  return 0;
}