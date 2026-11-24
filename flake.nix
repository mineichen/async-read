# NOTE for agents: you are inside `nix run .#isolated`, so the environment is built from the flake.nix.
# Edits to this file are NOT visible until the sandbox is restarted externally, you are expected not be able to run "nix run .#isolated" yourself
{
  description = "Deterministic Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    fenix.url = "github:nix-community/fenix";
    devtools.url = "github:mineichen/nix-devtools";
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];
      perSystem = { system, ... }:
        let
          pkgs = import inputs.nixpkgs {
            inherit system;
            config.allowUnfreePredicate = pkg: builtins.elem (inputs.nixpkgs.lib.getName pkg) [ "claude-code" ];
          };
          lib = pkgs.lib;
          fenix = inputs.fenix.packages.${system};

          # Latest stable for `nix develop` and `nix run .#isolated` (e.g. a current rust-analyzer)
          rust = fenix.stable.toolchain;
          # Pinned to the MSRV (`rust-version` in Cargo.toml), used by `nix run .#test`, also when
          # called from `nix develop` or `nix run .#isolated`. Update both together.
          rustMsrvChannel = {
            channel = "1.85.0";
            sha256 = "sha256-AJ6LX/Q/Er9kS15bn9iflkUwcgYqRQxiOIL2ToVAXaU=";
          };
          rustMsrv = (fenix.toolchainOf rustMsrvChannel).toolchain;
          rustNightlyWithMiri = fenix.latest.withComponents [ "rustc" "cargo" "miri" "rust-src" ];
          # A target without `std` and without an allocator, used by `nix run .#no-std`
          noStdTarget = "thumbv6m-none-eabi";
          rustNoStd = fenix.combine [
            fenix.stable.cargo
            fenix.stable.rustc
            fenix.targets.${noStdTarget}.stable.rust-std
          ];

          # Everything needed to build the workspace, besides the Rust toolchain
          buildTools = [
            pkgs.stdenv.cc
            pkgs.git
            pkgs.coreutils
          ];

          # Links the native binaries with the rpath of these libraries (e.g. libgcc_s), so no
          # LD_LIBRARY_PATH is needed. Not with RUSTFLAGS: cargo doesn't apply them to build scripts
          # and proc-macros with --target, so such a build would replace the native ones in target/.
          rpathLinker = pkgs.writeShellScript "cc-with-rpath" ''
            exec ${pkgs.stdenv.cc}/bin/cc ${lib.concatMapStringsSep " " (pkg: "-Wl,-rpath,${pkg}/lib") [
              pkgs.stdenv.cc.cc.lib
            ]} "$@"
          '';

          # The build environment of `nix develop`, `nix run .#isolated` and `nix run .#test`.
          # All of them build in target/, so they must not differ in a variable tracked by a build
          # script or cargo itself (e.g. CC, CXX, AR, PKG_CONFIG_PATH, SOURCE_DATE_EPOCH, ...).
          # Otherwise switching between them rebuilds the affected crates and all their dependents.
          # Toolchains of different versions (e.g. stable and MSRV) don't invalidate each other.
          devEnv = {
            "CARGO_TARGET_${pkgs.stdenv.hostPlatform.rust.cargoEnvVarTarget}_LINKER" = "${rpathLinker}";
            # Don't let the host's libraries shadow the rpath
            LD_LIBRARY_PATH = "";
            # `nix develop` always exports it (with this default), so the others need it as well
            SOURCE_DATE_EPOCH = "315532800";
          };
          # Cargo hashes the absolute path of the dependencies' sources into its fingerprints.
          # So CARGO_HOME must have the same path on the host and in `nix run .#isolated`,
          # whose HOME is an empty tmpfs. The workspace has the same path in both.
          setCargoHome = ''
            export CARGO_HOME="$(git rev-parse --show-toplevel)/target/cargo-home"
          '';
          greet = ''
            echo "Deterministic dev shell: $(rustc --version), $(cargo --version)"
          '';

          # A script which runs `commands` in the root of the workspace with `toolchain`. The
          # arguments of the script are the "$@" of `commands`.
          # Runs with exactly devEnv, so it builds the same on the host (e.g. in `nix develop`)
          # and in `nix run .#isolated`. Only variables that don't affect the build are passed on.
          inDevEnv = name: toolchain: commands:
            let
              script = pkgs.writeShellScript "${name}-in-dev-env" ''
                set -euo pipefail
                cd "$(git rev-parse --show-toplevel)"
                ${setCargoHome}
                rustc --version
                ${commands}
              '';
            in
            pkgs.writeShellScript name ''
              set -euo pipefail
              pass=()
              for var in HOME TERM SSL_CERT_FILE RUST_BACKTRACE CARGO_LOG; do
                if [ -n "''${!var-}" ]; then pass+=("$var=''${!var}"); fi
              done
              exec ${pkgs.coreutils}/bin/env -i "''${pass[@]}" \
                ${lib.escapeShellArgs (lib.mapAttrsToList (var: value: "${var}=${value}") devEnv)} \
                PATH=${lib.makeBinPath ([ toolchain ] ++ buildTools)} \
                ${script} "$@"
            '';

          # The whole test suite with the MSRV toolchain, used by `nix run .#test`, and with the
          # latest stable one, used by `nix run .#test-stable`. Arguments are passed to `cargo test`.
          testCommands = ''
            cargo test --workspace --all-features "$@"
          '';
          testScript = inDevEnv "test" rustMsrv testCommands;
          testStableScript = inDevEnv "test-stable" rust testCommands;

          # Neither clippy nor rustdoc may warn, with and without the features
          lintScript = inDevEnv "lint" rust ''
            for features in --all-features --no-default-features; do
              cargo clippy --workspace --all-targets "$features" -- --deny warnings
              RUSTDOCFLAGS="--deny warnings" cargo doc --workspace --no-deps "$features"
            done
          '';

          # Without its features, the crate builds for a target which has no `std`
          noStdScript = inDevEnv "no-std" rustNoStd ''
            cargo build --no-default-features --target ${noStdTarget} "$@"
          '';

          # Arguments are passed to `cargo miri test`. MIRIFLAGS is passed on as well, e.g.
          # `MIRIFLAGS=-Zmiri-tree-borrows nix run .#miri` for Tree instead of Stacked Borrows.
          miriScript = pkgs.writeShellScript "miri" ''
            export PATH="${rustNightlyWithMiri}/bin:${pkgs.stdenv.cc}/bin:$PATH"
            export LD_LIBRARY_PATH="${lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib ]}"
            exec ${rustNightlyWithMiri}/bin/cargo miri test --workspace --all-features "$@"
          '';
          coverageScript = inputs.devtools.lib.cargoTarpaulin {
            inherit pkgs rust;
            path = buildTools;
            env = devEnv;
          };

          isolated = inputs.devtools.lib.isolated {
            inherit pkgs inputs;
            name = "async-read-isolated-dev";
            packages = [ rust ] ++ buildTools ++ [
              pkgs.curl
              pkgs.findutils
              pkgs.diffutils
              pkgs.gnugrep
              pkgs.gnused
              pkgs.gawk
              pkgs.gnutar
              pkgs.gzip
              pkgs.which
              pkgs.less
              pkgs.procps
              pkgs.ripgrep
              pkgs.opencode
            ];
            claude.pluginDirs = [
              (inputs.devtools.lib.claudeRustAnalyzer {
                inherit pkgs;
                # The one of the development toolchain
                rust-analyzer = rust;
              })
            ];
            env = devEnv;
            # `nix run .#test`, `.#test-stable`, `.#lint`, `.#no-std`, `.#miri` and `.#coverage`
            # without downloading the toolchains
            extraStorePaths = [
              testScript
              testStableScript
              lintScript
              noStdScript
              miriScript
              coverageScript
            ];
            shellHook = greet + setCargoHome;
          };
        in
        {
          devShells.default = pkgs.mkShellNoCC {
            # Combined into one package, as the setup hooks of the packages themselves would export
            # CC, CXX, AR, ... which differ from `nix run .#isolated` (see devEnv)
            packages = [
              (pkgs.buildEnv {
                name = "async-read-dev-tools";
                paths = [ rust pkgs.bashInteractive ] ++ buildTools;
                pathsToLink = [ "/bin" ];
              })
            ];
            env = devEnv;
            shellHook = greet + setCargoHome;
          };

          apps.test = {
            type = "app";
            program = toString testScript;
          };

          apps.test-stable = {
            type = "app";
            program = toString testStableScript;
          };

          apps.lint = {
            type = "app";
            program = toString lintScript;
          };

          apps.no-std = {
            type = "app";
            program = toString noStdScript;
          };

          apps.miri = {
            type = "app";
            program = toString miriScript;
          };

          apps.coverage = {
            type = "app";
            program = toString coverageScript;
          };

          # nix run .#isolated [dir], see isolated.nix of nix-devtools
          apps.isolated = {
            type = "app";
            program = lib.getExe isolated;
          };
        };
    };
}
