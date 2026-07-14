{
  description = "burrow — distributed backup among friends, over iroh";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      overlay = final: prev: {
        burrow = final.rustPlatform.buildRustPackage {
          pname = "burrow";
          version = "0.1.0";
          src = self;
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = [ final.pkg-config ];
          # rusqlite is bundled; iroh needs no system libs on Linux/macOS.
          doCheck = true;
          meta = with final.lib; {
            description = "Distributed backup among friends — reserve space on each other's machines, over iroh";
            homepage = "https://github.com/solidsilver/burrow";
            license = with licenses; [ mit asl20 ];
            mainProgram = "burrow";
          };
        };
      };
    in
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ]
      (system:
        let pkgs = import nixpkgs { inherit system; overlays = [ overlay ]; };
        in {
          packages = {
            burrow = pkgs.burrow;
            default = pkgs.burrow;
          };
          devShells.default = pkgs.mkShell {
            inputsFrom = [ pkgs.burrow ];
            packages = with pkgs; [ rust-analyzer clippy rustfmt ];
          };
        })
    // {
      overlays.default = overlay;

      nixosModules.burrow = { config, lib, pkgs, ... }:
        let
          cfg = config.services.burrow;
          settingsFormat = pkgs.formats.toml { };
          configFile = settingsFormat.generate "burrow-config.toml" cfg.settings;
        in
        {
          options.services.burrow = {
            enable = lib.mkEnableOption "burrow distributed backup daemon";

            package = lib.mkOption {
              type = lib.types.package;
              default = pkgs.burrow or (import nixpkgs {
                inherit (pkgs) system;
                overlays = [ overlay ];
              }).burrow;
              defaultText = lib.literalExpression "pkgs.burrow";
              description = "The burrow package to run.";
            };

            user = lib.mkOption {
              type = lib.types.str;
              default = "burrow";
              description = ''
                User the daemon runs as. Must be able to read every path in
                settings.backup[].paths.
              '';
            };

            group = lib.mkOption {
              type = lib.types.str;
              default = "burrow";
              description = "Group the daemon runs as.";
            };

            dataDir = lib.mkOption {
              type = lib.types.path;
              default = "/var/lib/burrow";
              description = "Blob store, metadata database, and held peer data.";
            };

            settings = lib.mkOption {
              type = settingsFormat.type;
              default = { };
              example = lib.literalExpression ''
                {
                  node.name = "my-nas";
                  storage.offer_max = "500gb";
                  repair.grace_period = "72h";
                  backup = [{
                    id = "photos";
                    paths = [ "/tank/photos" ];
                    replicas = 3;
                    schedule = "0 3 * * *";
                    keep_last = 30;
                  }];
                }
              '';
              description = ''
                burrow configuration, mapped 1:1 onto config.toml.
                See contrib/config.example.toml for the full schema. Note that
                the repo key is state, not configuration: run `burrow init`
                once as the service user (or `burrow recover`), and store the
                printed recovery phrase somewhere safe and OFF this machine.
              '';
            };
          };

          config = lib.mkIf cfg.enable {
            users.users.${cfg.user} = lib.mkIf (cfg.user == "burrow") {
              isSystemUser = true;
              group = cfg.group;
              home = cfg.dataDir;
            };
            users.groups.${cfg.group} = lib.mkIf (cfg.group == "burrow") { };

            environment.systemPackages = [ cfg.package ];

            systemd.services.burrow = {
              description = "burrow — distributed backup among friends";
              wantedBy = [ "multi-user.target" ];
              after = [ "network-online.target" ];
              wants = [ "network-online.target" ];
              environment = {
                BURROW_DATA_DIR = "${cfg.dataDir}/data";
                BURROW_CONFIG_DIR = "${cfg.dataDir}/config";
              };
              preStart = ''
                mkdir -p "${cfg.dataDir}/data" "${cfg.dataDir}/config"
                ln -sf ${configFile} "${cfg.dataDir}/config/config.toml"
              '';
              serviceConfig = {
                ExecStart = "${lib.getExe cfg.package} daemon run";
                User = cfg.user;
                Group = cfg.group;
                Restart = "on-failure";
                RestartSec = 5;
                StateDirectory = "burrow";
                NoNewPrivileges = true;
                ProtectSystem = "strict";
                ReadWritePaths = [ cfg.dataDir ];
                # Backup sources are read via the daemon; grant read access:
                # add paths with services.burrow-extra ReadOnlyPaths or run as
                # a user that owns them.
                ProtectHome = lib.mkDefault "read-only";
                RestrictSUIDSGID = true;
                PrivateTmp = false; # control socket lives under /tmp
              };
            };
          };
        };

      nixosModules.default = self.nixosModules.burrow;
    };
}
