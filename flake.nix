{
  description = "temporal-workflow-kit dev environment (Go + Temporal CLI)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.go
            pkgs.temporal-cli
            pkgs.gopls
            pkgs.gotools
            # Runs the executable specifications under specs/. The plugins come
            # from nixpkgs rather than `gauge install`, so they are pinned by
            # flake.lock like everything else; the wrapped gauge refuses to
            # install them itself.
            (pkgs.gauge.withPlugins (p: [ p.go p.html-report ]))
          ];

          shellHook = ''
            export GOPATH="$HOME/go"
            export PATH="$GOPATH/bin:$PATH"
          '';
        };
      });
}
