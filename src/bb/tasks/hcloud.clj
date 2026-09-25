(ns tasks.hcloud
  (:require
   [babashka.cli :as cli]
   [babashka.process :as p]
   [clojure.string :as str]))

(def ^:private server-name "busker-bench")
(def ^:private remote-directory "/busker")
(def ^:private hcloud-command ["hcloud" "--context" "busker"])

(defn- hcloud!
  [& args]
  (apply p/shell (into hcloud-command args)))

(defn- server-ip []
  (-> (apply p/shell {:out :string}
             (into hcloud-command ["server" "ip" server-name]))
      :out
      str/trim))

(defn- ssh!
  [command]
  (hcloud! "server" "ssh" server-name
           "-o" "StrictHostKeyChecking=accept-new"
           command))

(defn create
  "Creates the Busker benchmark server."
  [_]
  (hcloud! "server" "create"
           "--name" server-name
           "--ssh-key" "casey"
           "--type" "ccx13"
           "--image" "ubuntu-26.04"))

(defn setup
  "Installs Nix and the benchmark tools pinned by flake.lock."
  [_]
  (ssh! (str "test -x /nix/var/nix/profiles/default/bin/nix-env || "
             "(curl -L https://nixos.org/nix/install | sh -s -- --daemon)"))
  (ssh! (str "mkdir -p " remote-directory "/bench"))
  (let [target (str "root@" (server-ip) ":" remote-directory)]
    (p/shell "scp" "flake.lock" (str target "/flake.lock"))
    (p/shell "scp" "bench/tools.nix" (str target "/bench/tools.nix")))
  (ssh! (str "/nix/var/nix/profiles/default/bin/nix-env -if "
             remote-directory "/bench/tools.nix")))

(defn upload
  "Uploads tracked benchmark files, excluding local credentials and build output."
  [_]
  (let [target (str "root@" (server-ip) ":" remote-directory)
        files (:out (p/shell {:out :string} "git" "ls-files" "-z"))]
    (println "Uploading tracked files to benchmarking server...")
    (p/shell {:in files} "rsync" "-az" "--from0" "--files-from=-"
             "--rsync-path=/root/.nix-profile/bin/rsync"
             "--exclude=.env*" "--exclude=/.git/***" "--exclude=/.scratch-org/***"
             "--exclude=/.pi/***" "--exclude=/.agents/***" "--exclude=/dev/***"
             "--exclude=/target/***" "--exclude=/shim/***" "." target)))

(defn bench
  "Uploads Busker and runs its Ring adapter benchmarks on the server."
  [opts]
  (upload opts)
  (ssh! (str "export PATH=/root/.nix-profile/bin:$PATH && cd " remote-directory
             " && clojure -X:deps prep :aliases '[:bench]'"
             " && timeout --signal=TERM --kill-after=10s 2h"
             " clojure -M:bench")))

(defn delete
  "Deletes the Busker benchmark server."
  [_]
  (hcloud! "server" "delete" server-name))

(defn- report-error
  [{:keys [msg wrong-input]}]
  (throw (ex-info (or msg (str "Unknown hcloud command: " wrong-input)) {})))

(defn run
  "Runs a Hetzner Cloud benchmark-server command."
  [& args]
  (if (or (empty? args)
          (some #{"-h" "--help"} args))
    (println (str "Usage: bb hcloud <command>\n\n"
                  "Commands:\n"
                  "  create  Create the benchmark server\n"
                  "  setup   Install benchmark prerequisites\n"
                  "  upload  Upload tracked benchmark files\n"
                  "  bench   Upload and run the benchmarks\n"
                  "  delete  Delete the benchmark server"))
    (cli/dispatch [{:cmds ["create"] :fn create :restrict true}
                   {:cmds ["setup"] :fn setup :restrict true}
                   {:cmds ["upload"] :fn upload :restrict true}
                   {:cmds ["bench"] :fn bench :restrict true}
                   {:cmds ["delete"] :fn delete :restrict true}]
                  args
                  {:error-fn report-error})))
