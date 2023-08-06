// extern crate cmake;
extern crate cmake;
use cmake::Config;

fn main() {
    // Builds the project in the directory located in `libdouble`, installing it
    // into $OUT_DIR
    // let dst = Config::new("NBTree")
    // .cflag("-std=c++11 ${CMAKE_CXX_FLAGS}  -DUSE_NVM_MALLOC -DCLEAR_NVM_POOL -g -O2 -mrtm -ldl -lpthread -lm")
    // .build();
    #[cfg(feature = "dash")]
    {
        let dst = cmake::build("dash");
        println!("cargo:rustc-link-search=native={}", dst.display());

        println!("cargo:rustc-link-search=./dash_pmdk/lib");

        println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu/");
        println!("cargo:rustc-link-search=/usr/lib64");

        // println!("cargo:rustc-link-search=./NBTree/third-party-lib/");
        // println!("cargo:rustc-link-search=./NBTree/include/");

        // println!("cargo:rustc-link-search=./NBTree/build");
        // println!("cargo:rustc-link-search=./dash");
        println!("cargo:rustc-link-search=./dash/build");

        // println!("cargo:rustc-link-lib=dylib=boost_system");
        // println!("cargo:rustc-link-lib=dylib=boost_thread");
        // println!("cargo:rustc-link-lib=dylib=pthread");
        println!("cargo:rustc-link-lib=dylib=pmemobj");
        println!("cargo:rustc-link-lib=dylib=pmem");
        println!("cargo:rustc-link-lib=dylib=stdc++");

        // println!("cargo:rustc-link-lib=dylib=jemalloc");
        // println!("cargo:rustc-link-lib=dylib=gcc");
        // println!("cargo:rustc-link-lib=dylib=tbb");
        // println!("cargo:rustc-link-lib=dylib=tbbmalloc_proxy");
        // println!("cargo:rustc-link-lib=dylib=tbbmalloc");
    }
    #[cfg(feature = "nbtree")]
    {
        let dst = cmake::build("NBTree");
        println!("cargo:rustc-link-search=native={}", dst.display());

        println!("cargo:rustc-link-search=./NBTree/third-party-lib/");
        println!("cargo:rustc-link-search=./NBTree/include/");

        println!("cargo:rustc-link-search=./NBTree/build");

        println!("cargo:rustc-link-lib=dylib=boost_system");
        println!("cargo:rustc-link-lib=dylib=boost_thread");
        println!("cargo:rustc-link-lib=dylib=pthread");
        println!("cargo:rustc-link-lib=dylib=pmemobj");
        println!("cargo:rustc-link-lib=dylib=pmem");
        println!("cargo:rustc-link-lib=dylib=stdc++");

        println!("cargo:rustc-link-lib=dylib=jemalloc");
        println!("cargo:rustc-link-lib=dylib=gcc");
        println!("cargo:rustc-link-lib=dylib=tbb");
        println!("cargo:rustc-link-lib=dylib=tbbmalloc_proxy");
        println!("cargo:rustc-link-lib=dylib=tbbmalloc");
    }
    // println!("cargo:rustc-link-lib=dylib=dash");

    // println!("cargo:rustc-link-lib=static=btree");
}
// fn main() {
//     println!("cargo:rustc-link-search=./src/c");
// }
