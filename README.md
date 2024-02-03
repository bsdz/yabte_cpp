# yabte_cpp - Yet Another BackTesting Engine - C++ Extensions

Reimplementation of YABTE project in C++ for faster back testing and batch running.

# License

This library is licensed under the Creative Commons Attribution-NonCommercial 4.0 International Public
License and is intended for personal use only. It cannot be used in a Bank, Hedgefund, Commodity House etc 
without prior permission from author (Blair Azzopardi).

# Set up

Use conan to build and prepare the dependencies (use clang profile for improved debugging).

```bash
conan install . --output-folder=build/Debug --build=missing --settings=build_type=Debug
conan install . --output-folder=build/Release --build=missing --settings=build_type=Release
```

To specify a different conan profile use switch, e.g. "--profile=clang".


Make the make files, use Release,

```bash
pushd build/Debug
cmake ../.. -DCMAKE_BUILD_TYPE=Debug
popd
pushd build/Release
cmake ../.. -DCMAKE_BUILD_TYPE=Release
popd
```

Once make files are made, one can build the code with (do this after code changes):

```bash
cmake --build . 
```

To clean build

```bash
cmake --build . --target clean
```


Run the test suite:

```bash
cd ./src_test
ctest
```

or directly to view logging:

```bash
./gtest_yabte | less
```