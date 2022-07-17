//
// Created by zhong on 5/31/2021.
//
#include <random>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <cmath>

#ifndef SCALABLE_INDEX_DATA_GENERATOR_H
#define SCALABLE_INDEX_DATA_GENERATOR_H
#define INCREASE_BOUND 1000
#define MUL_BOUND 1.00000012

#define NORMAL
//UNIFORM_MULTIPLY, UNIFORM_INCREASE, NORMAL

namespace pgmMetric {
  double VARIANCE=10;
  template<typename T>
  using LargeSigned = typename std::conditional_t<std::is_floating_point_v < T>,
  long double,
  std::conditional_t<(sizeof(T) < 8), int64_t, __int128>>;

  // Forward declaration
  template <class T>
  class LinearModelBuilder;

// Linear regression model
  template <class T>
  class LinearModel {
  public:
    double a_ = 0;  // slope
    double b_ = 0;  // intercept

    LinearModel() = default;
    LinearModel(double a, double b) : a_(a), b_(b) {}
    explicit LinearModel(const LinearModel& other) : a_(other.a_), b_(other.b_) {}

    void expand(double expansion_factor) {
      a_ *= expansion_factor;
      b_ *= expansion_factor;
    }

    inline uint64_t predict(T key) const {
      return static_cast<uint64_t>(a_ * static_cast<double>(key) + b_);
    }

    inline double predict_double(T key) const {
      return a_ * static_cast<double>(key) + b_;
    }

    void set(double a, double b) {
      a_ = a;
      b_ = b;
    }
  };

  template <class T>
  class LinearModelBuilder {
  public:
    LinearModel<T>* model_;

    explicit LinearModelBuilder<T>(LinearModel<T>* model) : model_(model) {}

    inline void reset() {
      count_ = 0;
      x_sum_ = 0;
      y_sum_ = 0;
      xx_sum_ = 0;
      xy_sum_ = 0;
      x_min_ = 0;
      x_max_ = 0;
      y_min_ = 0;
      y_max_ = 0;
    }

    inline void add(T x, uint64_t y) {
      count_++;
      x_sum_ += (__float128)(x);
      y_sum_ += (__float128)(y);
      xx_sum_ += (__float128)(x) * x;
      xy_sum_ += (__float128)(x) * y;
      x_min_ = std::min<T>(x, x_min_);
      x_max_ = std::max<T>(x, x_max_);
      y_min_ = std::min<long double>(y, y_min_);
      y_max_ = std::max<long double>(y, y_max_);
    }

    void build() {
      if (count_ <= 1) {
        model_->a_ = 0;
        model_->b_ = static_cast<long double>(y_sum_);
        return;
      }

      if ((__float128)(count_) * xx_sum_ - x_sum_ * x_sum_ == 0) {
        // all values in a bucket have the same key.
        model_->a_ = 0;
        model_->b_ = (__float128)(y_sum_) / count_;
        return;
      }

      auto slope = (__float128)(
              ((__float128)(count_) * xy_sum_ - x_sum_ * y_sum_) /
              ((__float128)(count_) * xx_sum_ - x_sum_ * x_sum_));
      auto intercept = (__float128)(
              (y_sum_ - (__float128)(slope) * x_sum_) / count_);
      model_->a_ = slope;
      model_->b_ = intercept;

      // If floating point precision errors, fit spline
      if (model_->a_ <= 0) {
        model_->a_ = (y_max_ - y_min_) / (x_max_ - x_min_);
        model_->b_ = -static_cast<long double>(x_min_) * model_->a_;
      }
    }

  private:
    int count_ = 0;
    __float128 x_sum_ = 0;
    __float128 y_sum_ = 0;
    __float128 xx_sum_ = 0;
    __float128 xy_sum_ = 0;
    T x_min_ = std::numeric_limits<T>::max();
    T x_max_ = std::numeric_limits<T>::lowest();
    long double y_min_ = std::numeric_limits<long double>::max();
    long double y_max_ = std::numeric_limits<long double>::lowest();
  };

  template<typename X, typename Y>
  class OptimalPiecewiseLinearModel {
  public:
    LinearModel<X> mse_model;
    LinearModelBuilder<X> mse_model_builder;

    std::vector<std::pair<X, Y>> points;
    std::mt19937 gen;

    using SX = LargeSigned<X>;
    using SY = LargeSigned<Y>;

    struct Slope {
      SX dx{};
      SY dy{};

      bool operator<(const Slope &p) const { return dy * p.dx < dx * p.dy; }

      bool operator>(const Slope &p) const { return dy * p.dx > dx * p.dy; }

      bool operator==(const Slope &p) const { return dy * p.dx == dx * p.dy; }

      bool operator!=(const Slope &p) const { return dy * p.dx != dx * p.dy; }

      explicit operator long double() const { return dy / (long double) dx; }
    };

    struct Point {
      X x{};
      Y y{};

      Slope operator-(const Point &p) const { return {SX(x) - p.x, SY(y) - p.y}; }
    };

    const Y epsilon;
    std::vector <Point> lower;
    std::vector <Point> upper;
    X first_x = 0;
    X last_x = 0;
    Y last_rank = 0;
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

    explicit OptimalPiecewiseLinearModel(Y epsilon, size_t seed = 2303649288) : epsilon(epsilon), lower(), upper(), mse_model_builder(&mse_model)  {
      if (epsilon < 0)
        throw std::invalid_argument("epsilon cannot be negative");

      upper.reserve(1u << 16);
      lower.reserve(1u << 16);

      gen.seed(seed);
    }


    bool one_point() const {
      return rectangle[0].x == rectangle[2].x && rectangle[0].y == rectangle[2].y
      && rectangle[1].x == rectangle[3].x && rectangle[1].y == rectangle[3].y;
    }

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
      auto b = (p0p1.dx * slope2.dy - p0p1.dy * slope2.dx) / static_cast<long double>(a);
      auto i_x = p0.x + b * slope1.dx;
      auto i_y = p0.y + b * slope1.dy;
      return {i_x, i_y};
    }

    std::pair<long double, SY> get_floating_point_segment(const X &origin) const {
      if (one_point())
        return {0, (rectangle[0].y + rectangle[1].y) / 2};

      if constexpr(std::is_integral_v < X > && std::is_integral_v < Y > )
      {
        auto slope = rectangle[3] - rectangle[1];
        auto intercept_n = slope.dy * (SX(origin) - rectangle[1].x);
        auto intercept_d = slope.dx;
        auto rounding_term = ((intercept_n < 0) ^ (intercept_d < 0) ? -1 : +1) * intercept_d / 2;
        auto intercept = (intercept_n + rounding_term) / intercept_d + rectangle[1].y;
        return {static_cast<long double>(slope), intercept};
      }

      auto[i_x, i_y] = get_intersection();
      auto[min_slope, max_slope] = get_slope_range();
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

    uint64_t generate_key() {
      uint64_t rank = last_rank + 1;

      uint64_t key;
      uint64_t max_key, min_key;
      if (points_in_hull == 1) {
#ifdef UNIFORM_MULTIPLY
        std::uniform_int_distribution <uint64_t> key_gen(last_x + 1, (last_x + 1) * MUL_BOUND);
        key = key_gen(gen);
#elif defined(UNIFORM_INCREASE)
        std::uniform_int_distribution <uint64_t> key_gen(last_x + 1, (last_x + 1) + INCREASE_BOUND);
        key = key_gen(gen);
#elif defined(NORMAL)
        std::normal_distribution<> key_gen{0,VARIANCE};
        key = last_x + 1 + std::round(std::fabs(key_gen(gen)));
#endif

      } else {
        double slope02 = (rectangle[2].y * 1.0 - rectangle[0].y * 1.0) / (rectangle[2].x * 1.0 - rectangle[0].x * 1.0);
        double intercept02 = rectangle[2].y * 1.0 - slope02 * rectangle[2].x * 1.0;
        double slope13 = (rectangle[1].y * 1.0 - rectangle[3].y * 1.0) / (rectangle[1].x * 1.0 - rectangle[3].x * 1.0);
        double intercept13 = rectangle[1].y * 1.0 - slope13 * rectangle[1].x;

        max_key = std::numeric_limits<uint64_t>::max();
        min_key = std::numeric_limits<uint64_t>::min();
        if (slope02 > 0) {
          max_key = ((rank + epsilon) - intercept02) / slope02;
//          max_key = (rank - intercept02) / slope02;
        }
#ifdef UNIFORM_MULTIPLY
        max_key = max_key <= (last_x + 1) * MUL_BOUND ? max_key : (last_x + 1) * MUL_BOUND;
#elif defined(UNIFORM_INCREASE)
        max_key = max_key <= (last_x + 1) + INCREASE_BOUND ? max_key : (last_x + 1) + INCREASE_BOUND;
#endif
//        min_key = (rank * 1.0 ) - intercept13 * 1.0 <= 0 ? 0 :
//                  ((rank * 1.0) - intercept13 * 1.0) / slope13;
        min_key = (rank * 1.0 - epsilon * 1.0) - intercept13 * 1.0 <= 0 ? 0 :
                  ((rank * 1.0 - epsilon * 1.0) - intercept13 * 1.0) / slope13;
        min_key = min_key >= last_x + 1 ? min_key : last_x + 1;
#ifdef UNIFORM_MULTIPLY
        std::uniform_int_distribution <uint64_t> key_gen(min_key, max_key);
        key = key_gen(gen);
#elif defined(USE_INCREASE)
        std::uniform_int_distribution <uint64_t> key_gen(min_key, max_key);
        key = key_gen(gen);
#elif defined(NORMAL)
        std::normal_distribution<> key_gen{0,VARIANCE};
        key = min_key + std::round(std::fabs(key_gen(gen)));
        key = key > max_key ? max_key : key;
#endif
      }

      return key;
    }

    uint64_t generate_key_out_of_bound() {
      uint64_t rank = last_rank + 1;
      double extreme_slope =
        (rectangle[2].y * 1.0 - rectangle[0].y * 1.0) / (rectangle[2].x * 1.0 - rectangle[0].x * 1.0);
      double interception = rectangle[2].y * 1.0 - extreme_slope * rectangle[2].x * 1.0;
      uint64_t key = ((rank + epsilon + 1) - interception) / extreme_slope;
      return key;
    }

    bool add_point(const X &x, const uint64_t &y) {
      if (points_in_hull > 0 && x <= last_x) {
        throw std::logic_error("Points must be increasing by x.");
      }

      last_x = x;
      last_rank = y;
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
        points.push_back(std::pair<X,Y>(x,y));
        return true;
      }

      if (points_in_hull == 1) {
        rectangle[2] = p2;
        rectangle[3] = p1;
        upper.push_back(p1);
        lower.push_back(p2);
        ++points_in_hull;
        points.push_back(std::pair<X,Y>(x,y));
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
        for (; end >= upper_start + 2 && cross(upper[end - 2], upper[end - 1], p1) <= 0; --end)
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
        for (; end >= lower_start + 2 && cross(lower[end - 2], lower[end - 1], p2) >= 0; --end)
          continue;
        lower.resize(end);
        lower.push_back(p2);
      }

      ++points_in_hull;
      points.push_back(std::pair<X,Y>(x,y));
      return true;
    }

    void reset() {
      points_in_hull = 0;
      lower.clear();
      upper.clear();
      mse_model_builder.reset();
      points.clear();
    }

    double get_mse_metric() {
      auto model_pair = get_floating_point_segment(first_x);
      long double slope = model_pair.first;
      uint64_t intercept = model_pair.second;

      long double sum = 0;

      int counter = 0;
      for (auto point : points) {
        counter++;
        auto predict_pos = (int64_t)(slope * (point.first - first_x)) + intercept;
        auto val = predict_pos - point.second;
        sum += val * val;
      }
      return sum / counter;
    }
  };

  template<typename KEY_TYPE>
  size_t PGM_metric(KEY_TYPE *keys, int key_num, int error_bound, double *mse = nullptr) {
    pgmMetric::OptimalPiecewiseLinearModel<KEY_TYPE, uint64_t> segment(error_bound);
    std::sort(keys, keys+key_num);
    size_t metric = 1;
    double mse_sum = 0;
    double max = 0;
    for(auto i = 0; i < key_num; i++) {
      if(!segment.add_point(keys[i], i)) {
        metric++;
        if(mse) {
          auto mse_segment = segment.get_mse_metric();
          mse_sum += mse_segment;
        }
        segment.reset();
        segment.add_point(keys[i], i);
      }
    }

    if(mse) {
//      std::cout << mse_sum << " " << metric << " " << max << std::endl;
      *mse = mse_sum / metric;
    }

    return metric;
  }

  template<typename KEY_TYPE>
  double skew_variance(KEY_TYPE *keys, int key_num, int error_bound, size_t count = 100000) {
    pgmMetric::OptimalPiecewiseLinearModel<KEY_TYPE, uint64_t> segment(error_bound);
    std::sort(keys, keys+key_num);
    size_t model = 0;
    double sum = 0;
    size_t seg_count = 0;
    for(auto i = 0; i < key_num; i++) {
      if(i % count == 0) {
        seg_count++;
        sum += model + 1;
        model = 0;
        segment.reset();
      }
      if(!segment.add_point(keys[i], i)) {
        model++;
        segment.reset();
        segment.add_point(keys[i], i);
      }
    }

    return (double) model / (double) seg_count;
  }

  void PGM_metric_data_generator(uint64_t *array, int data_num, int model_num, int error_bound = 64,
                                 size_t seed = 2303649288, std::string output_path = "", double variance=20) {
    uint64_t data_num_every_model = data_num / model_num;
    std::mt19937 gen(seed); //Standard mersenne_twister_engine seeded with rd()
    pgmMetric::OptimalPiecewiseLinearModel<uint64_t, uint64_t> optimal_segment(error_bound, seed);

    VARIANCE = variance;

    optimal_segment.reset();
    optimal_segment.add_point(0, 1);
    for (int i = 0; i < model_num; i++) {
      for (int j = 1; j < data_num_every_model; j++) {
        uint64_t key = optimal_segment.generate_key();
        uint64_t rank = i * data_num_every_model + j;
        array[rank] = key;
        optimal_segment.add_point(key, rank + 1);
      }

      uint64_t rank = (i + 1) * data_num_every_model;
      uint64_t new_key = optimal_segment.generate_key_out_of_bound();
      array[rank] = new_key;
      optimal_segment.reset();
      optimal_segment.add_point(new_key, rank + 1);


      if (i == model_num - 1) {
        for (int j = 0; j < data_num - data_num_every_model * model_num; j++) {
          uint64_t key = optimal_segment.generate_key();
          uint64_t rank = (i + 1) * data_num_every_model + j;
          array[rank] = key;
          optimal_segment.add_point(key, rank + 1);
        }
      }
    }
    std::shuffle(array, array + data_num, gen);
    if (output_path.size() > 0) {
      std::fstream file(output_path, std::ios::out | std::ios::binary);
      file.write(reinterpret_cast<char *>(array), sizeof(uint64_t) * data_num);
      file.close();
    }
  }

  void MSE_PGM_data_generator(uint64_t *array, int data_num, int model_num, int error_bound = 64,
                                 size_t seed = 2303649288, std::string output_path = "", double variance=20, double mse = 100, double mean = 0) {
    double mse_sum = 0;

    uint64_t data_num_every_model = data_num / model_num;
    std::mt19937 gen(seed); //Standard mersenne_twister_engine seeded with rd()
    pgmMetric::OptimalPiecewiseLinearModel<uint64_t, uint64_t> optimal_segment(error_bound, seed);

    VARIANCE = variance;

    optimal_segment.reset();
    optimal_segment.add_point(0, 1);

    std::uniform_real_distribution<double> slope_gen(1, std::numeric_limits<uint64_t>::max() / data_num / 1000);
    double slope = 1;
    double intercept = 0;
    uint64_t min_key = 0;
    uint64_t max_key = std::numeric_limits<uint64_t>::max();
    std::normal_distribution<> error{mean,sqrt(variance)};
    std::uniform_int_distribution<int> int_gen();


    for (int i = 0; i < model_num; i++) {
      slope = std::fabs(slope_gen(gen));
      LinearModel<uint64_t> model(slope, intercept);
      for (int j = 1; j < data_num_every_model; j++) {
        uint64_t rank = i * data_num_every_model + j;
        uint64_t key = model.predict(j);
        max_key = model.predict(j + error_bound);
        min_key = model.predict(j - error_bound) > min_key ? model.predict(j - error_bound) : min_key;

        double rank_error = std::fabs(error(gen));
        uint64_t key_error = static_cast<uint64_t>(rank_error * slope);

        mse_sum += rank_error * rank_error;

        key = key_error % 2 == 0 ? key + key_error : key - key_error;
        if(key > max_key || key < min_key + 1) {
          key = min_key + 1;
        }

        min_key = key;
        array[rank] = key;
        optimal_segment.add_point(key, rank + 1);
      }
      uint64_t rank = (i + 1) * data_num_every_model;

      uint64_t new_key = optimal_segment.generate_key_out_of_bound();
      min_key = new_key+1;
      max_key = std::numeric_limits<uint64_t>::max();
      array[rank] = new_key;
      intercept = new_key;
      optimal_segment.reset();
      optimal_segment.add_point(new_key, rank + 1);

      if (i == model_num - 1) {
        for (int j = 0; j < data_num - data_num_every_model * model_num; j++) {
          uint64_t key = model.predict(data_num_every_model + j);
          uint64_t rank = (i + 1) * data_num_every_model + j;
          array[rank] = key;
        }
      }
    }
    size_t pgm = PGM_metric(array, data_num, error_bound, &mse);
//    mse = mse_sum / data_num;
    std::cout << pgm << " " << mse << std::endl;
    std::shuffle(array, array + data_num, gen);


    if (output_path.size() > 0) {
      std::fstream file(output_path, std::ios::out | std::ios::binary);
      file.write(reinterpret_cast<char *>(array), sizeof(uint64_t) * data_num);
      file.close();
    }
  }


}


#endif //SCALABLE_INDEX_DATA_GENERATOR_H
