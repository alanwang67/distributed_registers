{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    parts.url = "github:hercules-ci/flake-parts";
    systems.url = "github:nix-systems/default";
    gomod2nix.url = "github:nix-community/gomod2nix";
    gomod2nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = inputs @ { self, nixpkgs, parts, systems, gomod2nix }: parts.lib.mkFlake { inherit inputs; } {
    systems = import inputs.systems;

    perSystem = { system, pkgs, ... }: {
      _module.args.pkgs = import nixpkgs {
        inherit system;
        overlays = [ gomod2nix.overlays.default ];
      };

      packages.default = pkgs.buildGoApplication {
        pname = "cs7670";
        version = self.shortRev or self.dirtyShortRev;
        src = ./.;
        modules = ./gomod2nix.toml;
      };

      devShells.default = pkgs.mkShell {
        packages = with pkgs; [
          delve
          go
          gopls
          gotools
          go-tools
          gomod2nix.packages.${system}.default
        ];
      };

      formatter = pkgs.writeShellScriptBin "formatter" ''
        set -eoux pipefail
        ${pkgs.nixpkgs-fmt}/bin/nixpkgs-fmt .
        ${pkgs.go}/bin/gofmt -s -w .
      '';
    };
  };
}
