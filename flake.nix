{
  description = "fabric";

  inputs = {
    nixpkgs.url = github:NixOS/nixpkgs/nixos-unstable;
    flakeutils = {
      url = "github:numtide/flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    naersk = {
      url = "github:nmattia/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flakeutils, rust-overlay, naersk }:
    flakeutils.lib.eachDefaultSystem (system:
      let
        VERSION = "0.1";

        overlays = [
          # rust-overlay.overlays.default
        ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
      in
      rec {
        devShell = pkgs.stdenv.mkDerivation rec {
          name = "semantics";
          src = self;

          nativeBuildInputs = with pkgs; [
            openssl
            # openssl.out
            # openssl.dev
          ];

          buildInputs = with pkgs; [
            pkg-config
          ] ++ uiBuildInputs ++ runtimeDeps;
          propagatedBuildInputs = with pkgs; [];
          runtimeDependencies = runtimeDeps;
          buildPhase = "";
          installPhase = "";

          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
            # Needed for the dioxus CLI
            pkgs.libz
            pkgs.stdenv.cc.cc.lib
            pkgs.openssl
          ];
          
          # Allow `cargo run` etc to find ssl lib.
          # LD_LIBRARY_PATH = "${pkgs.openssl.out}/lib:${pkgs.gtk3}/lib:${pkgs.webkitgtk}/lib:${pkgs.glib.out}/lib:${pkgs.stdenv.cc.cc.lib}/lib64:${pkgs.glib-networking}/lib";
          # LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
          RUST_BACKTRACE = "1";
          # RUST_LOG = "semantic=trace";

          # CARGO_NET_GIT_FETCH_WITH_CLI = "true";

          # Use mold linker for faster builds.
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = "clang";
          RUSTFLAGS = "-C link-arg=-fuse-ld=mold";
          UI_BUILD_DIR = "./ui/target/dx/semantic_ui/release/web/public";

          # Needed for building some C/C++ dependencies.
          # LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
        };
      }
    );
}
