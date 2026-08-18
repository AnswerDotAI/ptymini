fn main() {
    // glibc before 2.34 keeps openpty in libutil; later glibc and macOS ship an empty compat lib
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") && std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("gnu") {
        println!("cargo:rustc-link-lib=util");
    }
}
