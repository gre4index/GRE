# aux_source_directory(./src DIR_LIB_SRCS)

# set(CMAKE_CXX_STANDARD 14)
# set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_CXX_EXTENSIONS OFF)

# add_compile_options(-march=native -mtune=native -fopenmp -ljemalloc  -lnuma  -lpthread -lm -g -O3 -MD -MF -MP)

# include_directories(./src)

# add_library (MASSTREE ${DIR_LIB_SRCS})

aux_source_directory(./src DIR_LIB_SRCS)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "-march=native -mtune=native -Wall -Wextra -fopenmp -ltbb -g -mcx16 -latomic -Wno-invalid-offsetof -g -W -Wall -O3 -fPIC -include ./config.h -lpthread -lm")

add_compile_options(-faligned-new -march=native -mtune=native -march=native -Wall -Wextra -fopenmp -ltbb -g -mcx16 -latomic -Wno-invalid-offsetof -g -W -Wall -O3 -fPIC  -lpthread -lm -MD -MF -MP)

include_directories(./src)

add_library (MASSTREE ${DIR_LIB_SRCS})