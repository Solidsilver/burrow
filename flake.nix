{
  description = "burrow — distributed backup among friends, over iroh";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      overlay = final: prev: {
        # The Svelte SPA, built with npm. Output lands in the store and is
        # copied into crates/burrow-daemon/web-dist before the Rust build so
        # rust-embed picks it up (build.rs only writes the placeholder page
        # when web-dist is empty).
        burrow-web = final.buildNpmPackage {
          pname = "burrow-web";
          version = (builtins.fromJSON (builtins.readFile ./web/package.json)).version;
          src = ./web;
          npmDepsHash = "sha256-SL4RJbgloIV7yqFNZ+xq9heatXbzygdZonCOakVeI9s=";
          # vite.config.ts targets ../crates/burrow-daemon/web-dist for cargo
          # workflows; in the sandbox we redirect to a local dist/ instead.
          buildPhase = ''
            runHook preBuild
            npm run build -- --outDir dist
            runHook postBuild
          '';
          installPhase = ''
            runHook preInstall
            cp -r dist $out
            runHook postInstall
          '';
        };
        burrow = final.rustPlatform.buildRustPackage {
          pname = "burrow";
          version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
          src = self;
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = [ final.pkg-config ];
          preBuild = ''
            mkdir -p crates/burrow-daemon/web-dist
            cp -r ${final.burrow-web}/* crates/burrow-daemon/web-dist/
            chmod -R u+w crates/burrow-daemon/web-dist
          '';
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
            burrow-web = pkgs.burrow-web;
            default = pkgs.burrow;
          };
          apps =
            let burrowApp = {
              type = "app";
              program = "${pkgs.burrow}/bin/burrow";
            };
            in {
              burrow = burrowApp;
              default = burrowApp;
            };
          devShells.default = pkgs.mkShell {
            inputsFrom = [ pkgs.burrow ];
            packages = with pkgs; [ rust-analyzer clippy rustfmt nodejs ];
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
              description = ''
                Repo key, config, metadata database — and, unless blobsDir is
                set, the blob store too.
              '';
            };

            blobsDir = lib.mkOption {
              type = lib.types.nullOr lib.types.path;
              default = null;
              example = "/media/burrow";
              description = ''
                Store bulk blob data (own chunks + data held for friends)
                here instead of under dataDir — for keeping metadata on fast
                storage while blobs live on a large pool.
              '';
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

          config = lib.mkIf cfg.enable (
            let
              daemonEnv = {
                BURROW_DATA_DIR = "${cfg.dataDir}/data";
                BURROW_CONFIG_DIR = "${cfg.dataDir}/config";
              } // lib.optionalAttrs (cfg.blobsDir != null) {
                BURROW_BLOBS_DIR = cfg.blobsDir;
              };
              # Admin CLI against the system daemon: the control socket is
              # 0600 under the service user, so this re-executes as that user
              # with the daemon's environment.
              burrowctl = pkgs.writeShellScriptBin "burrowctl" ''
                exec /run/wrappers/bin/sudo -u ${cfg.user} \
                  ${lib.getBin pkgs.coreutils}/bin/env \
                  ${lib.concatStringsSep " "
                    (lib.mapAttrsToList (k: v: "${k}=${v}") daemonEnv)} \
                  ${lib.getExe cfg.package} "$@"
              '';
            in
            {
              users.users.${cfg.user} = lib.mkIf (cfg.user == "burrow") {
                isSystemUser = true;
                group = cfg.group;
                home = cfg.dataDir;
              };
              users.groups.${cfg.group} = lib.mkIf (cfg.group == "burrow") { };

              environment.systemPackages = [ cfg.package burrowctl ];

              # tmpfiles (not StateDirectory) so dataDir may live anywhere.
              systemd.tmpfiles.rules = [
                "d ${cfg.dataDir} 0750 ${cfg.user} ${cfg.group} -"
                "d ${cfg.dataDir}/data 0750 ${cfg.user} ${cfg.group} -"
                "d ${cfg.dataDir}/config 0750 ${cfg.user} ${cfg.group} -"
              ] ++ lib.optional (cfg.blobsDir != null)
                "d ${cfg.blobsDir} 0750 ${cfg.user} ${cfg.group} -";

              systemd.services.burrow = {
                description = "burrow — distributed backup among friends";
                wantedBy = [ "multi-user.target" ];
                after = [ "network-online.target" ];
                wants = [ "network-online.target" ];
                environment = daemonEnv;
                preStart = ''
                  ln -sf ${configFile} "${cfg.dataDir}/config/config.toml"
                '';
                serviceConfig = {
                  ExecStart = "${lib.getExe cfg.package} daemon run";
                  User = cfg.user;
                  Group = cfg.group;
                  Restart = "on-failure";
                  RestartSec = 5;
                  NoNewPrivileges = true;
                  ProtectSystem = "strict";
                  # /tmp must be writable through ProtectSystem=strict: the CLI
                  # control socket lives at /tmp/burrow-<uid>/<hash>.sock (that
                  # is also why PrivateTmp is off below).
                  ReadWritePaths = [ cfg.dataDir "/tmp" ]
                    ++ lib.optional (cfg.blobsDir != null) cfg.blobsDir;
                  # Backup sources are read via the daemon; grant read access:
                  # add paths with services.burrow-extra ReadOnlyPaths or run as
                  # a user that owns them.
                  ProtectHome = lib.mkDefault "read-only";
                  RestrictSUIDSGID = true;
                  PrivateTmp = false; # control socket lives under /tmp
                };
              };
            }
          );
        };

      nixosModules.default = self.nixosModules.burrow;
    };
}
