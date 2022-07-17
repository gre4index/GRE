aux_source_directory(./src DIR_LIB_SRCS)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include_directories(.)
add_compile_options(-std=gnu18  -march=native -mtune=native -pthread -Wall -Wextra -Wshadow -flto -fno-builtin-memcpy -fno-builtin-memmove -fno-builtin-memcmp -DNDEBUG -g3 -O3 -fno-stack-protector -fmax-errors=3 -Wno-unknown-pragmas  -rdynamic -lm -lrt)

add_library (WORMHOLE ${DIR_LIB_SRCS})